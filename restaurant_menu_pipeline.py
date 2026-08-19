# Databricks notebook source
# MAGIC %md
# MAGIC # Restaurant menu pipeline → Unity Catalog
# MAGIC
# MAGIC Discovers restaurants with the **Google Places API**, fetches each restaurant's own
# MAGIC website to find its menu, extracts structured menu items with **Databricks AI
# MAGIC functions** (`ai_parse_document` + `ai_query`), and lands everything in **Unity Catalog**.
# MAGIC
# MAGIC It is fully parameterised through the widgets below, uses only the Python standard
# MAGIC library (no `pip install`), and reads the Google key from a Databricks **secret** so
# MAGIC nothing sensitive lives in the notebook.
# MAGIC
# MAGIC **What you get in Unity Catalog** (`<catalog>.<schema>`):
# MAGIC | Object | Layer | Contents |
# MAGIC |---|---|---|
# MAGIC | `raw` (Volume) | raw | Fetched HTML / PDF bytes, for provenance |
# MAGIC | `restaurants_bronze` | bronze | Raw Places API JSON, one row per place |
# MAGIC | `restaurants_silver` | silver | Cleaned metadata, one row per place (staging) |
# MAGIC | `menu_documents` | silver | Each fetched source and its fetch outcome |
# MAGIC | `menu_items` | gold | Structured line items (section / item / price) |
# MAGIC | `restaurants` | gold | Metadata + `menu_status` + `menu_item_count` (query this) |
# MAGIC
# MAGIC **Honest expectation:** every restaurant gets a metadata row, but not every one yields a
# MAGIC parseable menu. Many sites hide menus inside JavaScript ordering widgets or images.
# MAGIC `menu_status` tells you exactly what happened for each restaurant.
# MAGIC
# MAGIC **Prerequisites** (see README): a Google Places API key stored as a Databricks secret,
# MAGIC and serverless internet egress enabled (run `00_stage0_probe` first to confirm).

# COMMAND ----------
# MAGIC %md ## Parameters

# COMMAND ----------
# Widgets make the notebook reusable across workspaces / customers.
dbutils.widgets.text("catalog", "deep_test_1_catalog", "1. Catalog")
dbutils.widgets.text("schema", "restaurant_menus", "2. Schema")
dbutils.widgets.text("secret_scope", "restaurant_pipeline", "3. Secret scope")
dbutils.widgets.text("secret_key", "google_places_api_key", "4. Secret key")
dbutils.widgets.text(
    "search_queries",
    # Comma-separated Places text queries. Default = South East QLD around Brisbane.
    "restaurants in Brisbane CBD QLD, restaurants in Fortitude Valley QLD, "
    "restaurants in South Bank Brisbane, restaurants in West End Brisbane QLD, "
    "restaurants in New Farm Brisbane, restaurants in Paddington Brisbane QLD, "
    "restaurants in Gold Coast QLD, restaurants in Surfers Paradise QLD, "
    "restaurants in Broadbeach QLD, restaurants in Sunshine Coast QLD, "
    "restaurants in Noosa QLD, restaurants in Ipswich QLD, "
    "restaurants in Logan Central QLD, restaurants in Redcliffe QLD",
    "5. Search queries (comma-sep)",
)
dbutils.widgets.text("target_count", "200", "6. Target # restaurants")
dbutils.widgets.text("max_sites_to_fetch", "200", "7. Max sites to fetch (cost/time cap)")
dbutils.widgets.text("extraction_model", "databricks-meta-llama-3-3-70b-instruct", "8. Extraction model")
dbutils.widgets.text("default_currency", "AUD", "9. Currency (single region per run)")
dbutils.widgets.dropdown("reset_tables", "false", ["true", "false"], "10. Drop & rebuild tables")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY = dbutils.widgets.get("secret_key").strip()
SEARCH_QUERIES = [q.strip() for q in dbutils.widgets.get("search_queries").split(",") if q.strip()]
TARGET_COUNT = int(dbutils.widgets.get("target_count"))
MAX_SITES = int(dbutils.widgets.get("max_sites_to_fetch"))
MODEL = dbutils.widgets.get("extraction_model").strip()
DEFAULT_CURRENCY = dbutils.widgets.get("default_currency").strip() or "AUD"
RESET = dbutils.widgets.get("reset_tables") == "true"

FQ = f"`{CATALOG}`.`{SCHEMA}`"          # fully-qualified schema
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
USER_AGENT = "DatabricksRestaurantMenuBot/1.0 (+contact your data team; respects robots.txt)"

print(f"Catalog.Schema : {CATALOG}.{SCHEMA}")
print(f"Queries        : {len(SEARCH_QUERIES)}  |  target={TARGET_COUNT}  fetch_cap={MAX_SITES}")
print(f"Model          : {MODEL}")
print(f"Reset tables   : {RESET}")

# COMMAND ----------
# MAGIC %md ## Setup: schema, volume, imports

# COMMAND ----------
import json, time, socket, ssl, io
import urllib.request, urllib.error, urllib.parse
from urllib import robotparser
from html.parser import HTMLParser
from datetime import datetime, timezone

socket.setdefaulttimeout(25)

# Catalog is assumed to exist (create requires CREATE CATALOG). Schema + volume we create.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
except Exception as e:
    print(f"(Catalog create skipped — assuming `{CATALOG}` already exists: {str(e)[:120]})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.raw")

if RESET:
    for t in ["menu_items", "menu_documents", "restaurants", "restaurants_silver", "restaurants_bronze"]:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t}")
    print("Dropped existing tables (reset_tables=true).")

print("Schema and volume ready.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 1 — Discover restaurants (Google Places API)
# MAGIC Runs each text query, paginates, dedupes on `place_id`, stops at the target count.

# COMMAND ----------
FIELD_MASK = ",".join([
    "places.id", "places.displayName", "places.formattedAddress",
    "places.location", "places.rating", "places.userRatingCount",
    "places.priceLevel", "places.types", "places.primaryType",
    "places.websiteUri", "places.nationalPhoneNumber",
    "places.currentOpeningHours.weekdayDescriptions", "places.businessStatus",
    "nextPageToken",
])

def places_text_search(api_key, query, page_token=None):
    """One page of Places Text Search (New). Returns (places, next_page_token)."""
    body = {"textQuery": query, "pageSize": 20}
    if page_token:
        body["pageToken"] = page_token
    req = urllib.request.Request(
        "https://places.googleapis.com/v1/places:searchText",
        data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json",
                 "X-Goog-Api-Key": api_key,
                 "X-Goog-FieldMask": FIELD_MASK},
    )
    with urllib.request.urlopen(req, timeout=25) as r:
        data = json.loads(r.read())
    return data.get("places", []), data.get("nextPageToken")

def flatten_place(p):
    loc = p.get("location") or {}
    hours = (p.get("currentOpeningHours") or {}).get("weekdayDescriptions") or []
    return {
        "place_id": p.get("id"),
        "name": (p.get("displayName") or {}).get("text"),
        "formatted_address": p.get("formattedAddress"),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "primary_type": p.get("primaryType"),
        "types": ",".join(p.get("types") or []),
        "rating": p.get("rating"),
        "user_rating_count": p.get("userRatingCount"),
        "price_level": p.get("priceLevel"),
        "website_uri": p.get("websiteUri"),
        "phone": p.get("nationalPhoneNumber"),
        "opening_hours": " | ".join(hours) if hours else None,
        "business_status": p.get("businessStatus"),
        "raw_json": json.dumps(p),
    }

api_key = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)  # auto-redacted in output
found = {}   # place_id -> flattened record
for q in SEARCH_QUERIES:
    if len(found) >= TARGET_COUNT:
        break
    token, pages = None, 0
    while pages < 3 and len(found) < TARGET_COUNT:      # Places caps ~60 results/query
        try:
            places, token = places_text_search(api_key, q, token)
        except urllib.error.HTTPError as e:
            print(f"  ! '{q}' page {pages}: HTTP {e.code} {e.read().decode()[:160]}")
            break
        except Exception as e:
            print(f"  ! '{q}' page {pages}: {type(e).__name__} {e}")
            break
        for p in places:
            rec = flatten_place(p)
            if rec["place_id"] and rec["place_id"] not in found:
                found[rec["place_id"]] = rec
        pages += 1
        print(f"  '{q}': +{len(places)} (total unique {len(found)})")
        if not token:
            break
        time.sleep(2)   # let the next page token settle

records = list(found.values())[:TARGET_COUNT]
print(f"\nDiscovered {len(records)} unique restaurants.")

# COMMAND ----------
from pyspark.sql import types as T
from pyspark.sql import functions as F

bronze_schema = T.StructType([
    T.StructField("place_id", T.StringType()),
    T.StructField("raw_json", T.StringType()),
])
silver_schema = T.StructType([
    T.StructField("place_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("formatted_address", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("primary_type", T.StringType()),
    T.StructField("types", T.StringType()),
    T.StructField("rating", T.DoubleType()),
    T.StructField("user_rating_count", T.LongType()),
    T.StructField("price_level", T.StringType()),
    T.StructField("website_uri", T.StringType()),
    T.StructField("phone", T.StringType()),
    T.StructField("opening_hours", T.StringType()),
    T.StructField("business_status", T.StringType()),
])

bronze_df = (spark.createDataFrame([{"place_id": r["place_id"], "raw_json": r["raw_json"]} for r in records],
                                   schema=bronze_schema)
             .withColumn("ingested_at", F.current_timestamp()))
bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.restaurants_bronze")

# Silver metadata goes to a staging table. The final `restaurants` table (silver + menu_status)
# is built once, at the end, in Stage 4 - so a run that stops early never leaves `restaurants`
# half-built without its status columns.
silver_rows = [{k: r[k] for k in silver_schema.fieldNames()} for r in records]
silver_df = spark.createDataFrame(silver_rows, schema=silver_schema)
silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.restaurants_silver")
print(f"Wrote {FQ}.restaurants_bronze and {FQ}.restaurants_silver ({len(records)} rows).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 2 — Fetch each restaurant's site & locate the menu
# MAGIC Honours `robots.txt`, uses a descriptive User-Agent, adds polite delays, stores raw
# MAGIC bytes in the Volume, and captures the outcome per restaurant.

# COMMAND ----------
class _Extract(HTMLParser):
    """Stdlib HTML → visible text + links. No third-party dependency."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._skip = 0
        self.chunks, self.links = [], []
        self._href = None
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript", "svg"):
            self._skip += 1
        if tag == "a":
            for k, v in attrs:
                if k == "href" and v:
                    self._href = v
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "svg") and self._skip:
            self._skip -= 1
    def handle_data(self, data):
        if not self._skip:
            t = data.strip()
            if t:
                self.chunks.append(t)
                if self._href:
                    self.links.append((self._href, t))
    def text(self):
        return " ".join(self.chunks)

def robots_ok(url, ua=USER_AGENT):
    try:
        parts = urllib.parse.urlparse(url)
        rp = robotparser.RobotFileParser()
        rp.set_url(f"{parts.scheme}://{parts.netloc}/robots.txt")
        rp.read()
        return rp.can_fetch(ua, url)
    except Exception:
        # Intentional fail-open: a missing or unreadable robots.txt is the conventional
        # "crawling allowed" case. Flip this to False if you want strict opt-in only.
        return True

def fetch(url, ua=USER_AGENT, timeout=20, max_bytes=3_000_000):
    req = urllib.request.Request(url, headers={"User-Agent": ua, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, (r.headers.get_content_type() or ""), r.read(max_bytes)

MENU_HINTS = ("menu", "menus", "dinner", "lunch", "drinks", "food", "eat", "carte")

def pick_menu_candidates(base_url, links):
    """Rank same-site links that look like a menu; keep PDFs and 'menu' pages first."""
    base = urllib.parse.urlparse(base_url)
    cands, seen = [], set()
    for href, text in links:
        absu = urllib.parse.urljoin(base_url, href)
        pu = urllib.parse.urlparse(absu)
        if pu.scheme not in ("http", "https") or pu.netloc != base.netloc:
            continue
        blob = f"{href} {text}".lower()
        is_pdf = absu.lower().split("?")[0].endswith(".pdf")
        if any(h in blob for h in MENU_HINTS) or is_pdf:
            score = (0 if is_pdf and "menu" in blob else 1 if "menu" in blob else 2)
            if absu not in seen:
                seen.add(absu)
                cands.append((score, absu))
    cands.sort(key=lambda x: x[0])
    return [u for _, u in cands[:3]]

def store_bytes(place_id, filename, content):
    import os
    d = f"{VOLUME_PATH}/{place_id}"
    os.makedirs(d, exist_ok=True)
    path = f"{d}/{filename}"
    with open(path, "wb") as f:
        f.write(content)
    return path

def slugify(url):
    return urllib.parse.quote(url, safe="")[:120]

docs = []          # menu_documents rows
to_fetch = [r for r in records if r.get("website_uri")][:MAX_SITES]
print(f"Fetching {len(to_fetch)} sites (of {len(records)} restaurants; rest have no website).")

for i, r in enumerate(to_fetch, 1):
    pid, site = r["place_id"], r["website_uri"]
    row = {"place_id": pid, "source_url": site, "doc_type": "none", "http_status": None,
           "content_type": None, "stored_path": None, "raw_text": None,
           "robots_allowed": True, "note": None}
    try:
        if not robots_ok(site):
            row.update(doc_type="robots_blocked", robots_allowed=False, note="homepage disallowed")
            docs.append(row); continue
        status, ctype, content = fetch(site)
        row.update(http_status=status, content_type=ctype)
        home_path = store_bytes(pid, "homepage.html", content)
        ex = _Extract();
        try: ex.feed(content.decode("utf-8", "ignore"))
        except Exception: pass
        candidates = pick_menu_candidates(site, ex.links)

        got = False
        for c in candidates:
            if not robots_ok(c):
                continue
            try:
                s2, ct2, c2 = fetch(c)
            except Exception:
                continue
            if "pdf" in ct2 or c.lower().split("?")[0].endswith(".pdf"):
                p = store_bytes(pid, f"menu_{slugify(c)}.pdf", c2)
                row.update(doc_type="pdf", stored_path=p, source_url=c, content_type=ct2, http_status=s2)
                got = True; break
            if "html" in ct2 or ct2 == "":
                mex = _Extract()
                try: mex.feed(c2.decode("utf-8", "ignore"))
                except Exception: pass
                txt = mex.text()
                if len(txt) > 200:
                    p = store_bytes(pid, f"menu_{slugify(c)}.html", c2)
                    row.update(doc_type="html", stored_path=p, source_url=c,
                               content_type=ct2, http_status=s2, raw_text=txt[:20000])
                    got = True; break
        if not got:
            # Fall back to the homepage text itself (some sites put the menu inline).
            htxt = ex.text()
            if len(htxt) > 200:
                row.update(doc_type="html", stored_path=home_path, raw_text=htxt[:20000],
                           note="homepage fallback")
            else:
                row.update(doc_type="none", note="no menu link / thin homepage (likely JS widget)")
    except urllib.error.HTTPError as e:
        row.update(doc_type="fetch_failed", http_status=e.code, note=str(e)[:160])
    except Exception as e:
        row.update(doc_type="fetch_failed", note=f"{type(e).__name__}: {str(e)[:160]}")
    docs.append(row)
    if i % 20 == 0:
        print(f"  fetched {i}/{len(to_fetch)}")
    time.sleep(0.6)   # be polite

print(f"Fetch complete. {sum(1 for d in docs if d['doc_type'] in ('html','pdf'))} sites yielded a menu source.")

# COMMAND ----------
docs_schema = T.StructType([
    T.StructField("place_id", T.StringType()),
    T.StructField("source_url", T.StringType()),
    T.StructField("doc_type", T.StringType()),
    T.StructField("http_status", T.IntegerType()),
    T.StructField("content_type", T.StringType()),
    T.StructField("stored_path", T.StringType()),
    T.StructField("raw_text", T.StringType()),
    T.StructField("robots_allowed", T.BooleanType()),
    T.StructField("note", T.StringType()),
])
docs_df = spark.createDataFrame(docs, schema=docs_schema) if docs else spark.createDataFrame([], docs_schema)
docs_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.menu_documents")
print(f"Wrote {FQ}.menu_documents ({len(docs)} rows).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 3 — Extract structured menus (Databricks AI)
# MAGIC PDFs/images go through `ai_parse_document`; the resulting text (and inline HTML text)
# MAGIC is structured into menu items by `ai_query`. The prompt forbids inventing items.

# COMMAND ----------
from pyspark.sql import functions as F

# 3a. Assemble source text. HTML text is already inline; PDFs get parsed with ai_parse_document.
html_src = spark.table(f"{FQ}.menu_documents").where("doc_type = 'html' AND raw_text IS NOT NULL") \
    .select("place_id", "source_url", F.col("raw_text").alias("source_text"))

pdf_docs = spark.table(f"{FQ}.menu_documents").where("doc_type = 'pdf' AND stored_path IS NOT NULL")
pdf_paths = [r["stored_path"] for r in pdf_docs.select("stored_path").collect()]

source_text = html_src
if pdf_paths:
    try:
        # ai_parse_document turns the binary into structured content; to_json flattens it for the LLM.
        # Join back on a scheme-independent key (everything after '/raw/').
        parsed = (spark.read.format("binaryFile").load(pdf_paths)
                  .withColumn("key", F.regexp_extract("path", "raw/(.*)$", 1))
                  .selectExpr("key", "substr(to_json(ai_parse_document(content)), 1, 20000) AS source_text"))
        pdf_keyed = pdf_docs.withColumn("key", F.regexp_extract("stored_path", "raw/(.*)$", 1))
        pdf_src = (parsed.join(pdf_keyed.select("place_id", "source_url", "key"), "key")
                   .select("place_id", "source_url", "source_text"))
        source_text = html_src.unionByName(pdf_src)
        print(f"Parsed {len(pdf_paths)} PDF menu(s) with ai_parse_document.")
    except Exception as e:
        print(f"(PDF parsing skipped — ai_parse_document error, continuing with HTML sources: {str(e)[:200]})")

source_text = source_text.where("length(source_text) > 200")
source_text.createOrReplaceTempView("menu_source_text")
n_src = source_text.count()
print(f"{n_src} documents queued for menu extraction.")

# COMMAND ----------
# 3b. Structured extraction with ai_query using a JSON-schema responseFormat.
# ai_query returns a JSON string conforming to the schema; we parse it with from_json.
# Currency is set deterministically per run (see DEFAULT_CURRENCY), not guessed by the model,
# because a model asked to infer currency from menu text will sometimes get it wrong.
MENU_DDL = ("STRUCT<items: ARRAY<STRUCT<section: STRING, item_name: STRING, "
            "description: STRING, price: DOUBLE>>>")
RESPONSE_FORMAT = json.dumps({
    "type": "json_schema",
    "json_schema": {
        "name": "menu_extraction",
        "schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "section": {"type": "string"},
                            "item_name": {"type": "string"},
                            "description": {"type": "string"},
                            "price": {"type": ["number", "null"]},
                        },
                        "required": ["section", "item_name", "description", "price"],
                    },
                }
            },
            "required": ["items"],
        },
        "strict": True,
    },
})

# Built by concatenation (the schema JSON contains braces that would break an f-string).
extract_sql = (
    "SELECT place_id, source_url, ai_query('" + MODEL + "', "
    "CONCAT('You are extracting a restaurant menu from raw website or document text. "
    "Return ONLY dishes or drinks explicitly present in the text as menu items. "
    "Do NOT invent items, sections, or prices. If a price is not shown set price to null. "
    "Ignore navigation, cookie banners, addresses, phone numbers and reviews. "
    "If the text is not a food or drink menu return an empty items array. "
    "Text:\\n', source_text), "
    "responseFormat => '" + RESPONSE_FORMAT + "') AS menu_json "
    "FROM menu_source_text"
)

if n_src > 0:
    extracted = spark.sql(extract_sql).withColumn("parsed", F.from_json("menu_json", MENU_DDL))
    items = (extracted
             .withColumn("item", F.explode_outer("parsed.items"))
             .select("place_id", "source_url",
                     F.col("item.section").alias("section"),
                     F.col("item.item_name").alias("item_name"),
                     F.col("item.description").alias("description"),
                     F.col("item.price").alias("price"))
             .where("item_name IS NOT NULL AND length(trim(item_name)) > 0")
             # Drop exact repeats (same item on more than one fetched page). Price variants
             # (glass/bottle) survive because price is part of the key.
             .dropDuplicates(["place_id", "section", "item_name", "description", "price"])
             .withColumn("currency", F.lit(DEFAULT_CURRENCY))
             .withColumn("extracted_at", F.current_timestamp()))
    items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.menu_items")
    n_items = spark.table(f"{FQ}.menu_items").count()
else:
    spark.createDataFrame([], T.StructType([
        T.StructField("place_id", T.StringType()), T.StructField("source_url", T.StringType()),
        T.StructField("section", T.StringType()), T.StructField("item_name", T.StringType()),
        T.StructField("description", T.StringType()), T.StructField("price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
    ])).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.menu_items")
    n_items = 0
print(f"Wrote {FQ}.menu_items ({n_items} line items).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage 4 — Finalise `menu_status` and summarise

# COMMAND ----------
# Build the final `restaurants` table from the silver staging table (NOT from itself), so this
# is a clean one-way write with no self-reference. `restaurants` gets its status columns here.
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.restaurants AS
WITH item_counts AS (
  SELECT place_id, COUNT(*) AS n_items FROM {FQ}.menu_items GROUP BY place_id
),
doc_status AS (
  SELECT place_id,
         MAX(CASE WHEN doc_type IN ('html','pdf') THEN 1 ELSE 0 END) AS got_source,
         MAX(CASE WHEN doc_type = 'robots_blocked' THEN 1 ELSE 0 END) AS blocked,
         MAX(CASE WHEN doc_type = 'fetch_failed' THEN 1 ELSE 0 END) AS failed
  FROM {FQ}.menu_documents GROUP BY place_id
)
SELECT r.*,
  CASE
    WHEN r.website_uri IS NULL THEN 'no_website'
    WHEN COALESCE(c.n_items,0) > 0 THEN 'menu_extracted'
    WHEN COALESCE(d.got_source,0) = 1 THEN 'source_found_no_items'
    WHEN COALESCE(d.blocked,0) = 1 THEN 'robots_blocked'
    WHEN COALESCE(d.failed,0) = 1 THEN 'fetch_failed'
    ELSE 'no_menu_found'
  END AS menu_status,
  COALESCE(c.n_items, 0) AS menu_item_count
FROM {FQ}.restaurants_silver r
LEFT JOIN item_counts c ON r.place_id = c.place_id
LEFT JOIN doc_status  d ON r.place_id = d.place_id
""")

status = {r["menu_status"]: r["n"] for r in
          spark.sql(f"SELECT menu_status, COUNT(*) n FROM {FQ}.restaurants GROUP BY menu_status").collect()}
summary = {
    "catalog_schema": f"{CATALOG}.{SCHEMA}",
    "restaurants": spark.table(f"{FQ}.restaurants").count(),
    "with_website": spark.table(f"{FQ}.restaurants").where("website_uri IS NOT NULL").count(),
    "menu_items": spark.table(f"{FQ}.menu_items").count(),
    "restaurants_with_menu": status.get("menu_extracted", 0),
    "status_breakdown": status,
}
print(json.dumps(summary, indent=2))

# COMMAND ----------
display(spark.sql(f"""
  SELECT menu_status, COUNT(*) AS restaurants, SUM(menu_item_count) AS total_items
  FROM {FQ}.restaurants GROUP BY menu_status ORDER BY restaurants DESC
"""))

# COMMAND ----------
display(spark.sql(f"""
  SELECT r.name, r.rating, i.section, i.item_name, i.price, i.currency
  FROM {FQ}.menu_items i JOIN {FQ}.restaurants r USING (place_id)
  ORDER BY r.name LIMIT 50
"""))

# COMMAND ----------
dbutils.notebook.exit(json.dumps(summary, default=str))
