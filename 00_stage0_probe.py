# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 0 - Feasibility probe
# MAGIC Confirms, before we build the full pipeline, that this workspace can:
# MAGIC 1. reach the public internet from serverless compute (egress),
# MAGIC 2. call the Google Places API with the stored secret,
# MAGIC 3. run `ai_query` against a foundation model,
# MAGIC 4. resolve `ai_parse_document`.
# MAGIC
# MAGIC It prints a report and returns it as JSON via `dbutils.notebook.exit`.

# COMMAND ----------

import json, urllib.request, urllib.error, traceback

report = {}

def _http(url, data=None, headers=None, method=None, timeout=25):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()

# COMMAND ----------
# 1) Generic egress: what public IP do we exit from?
try:
    status, body = _http("https://api.ipify.org?format=json")
    report["egress_generic"] = {"ok": True, "status": status, "egress_ip": json.loads(body).get("ip")}
except Exception as e:
    report["egress_generic"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

# COMMAND ----------
# 2) Google Places API (New) - Text Search. Secret is auto-redacted by Databricks.
try:
    api_key = dbutils.secrets.get("restaurant_pipeline", "google_places_api_key")
    payload = json.dumps({"textQuery": "restaurants in Brisbane", "pageSize": 5}).encode()
    field_mask = ",".join([
        "places.id", "places.displayName", "places.formattedAddress",
        "places.location", "places.rating", "places.userRatingCount",
        "places.priceLevel", "places.types", "places.primaryType",
        "places.websiteUri", "places.nationalPhoneNumber",
        "places.currentOpeningHours.weekdayDescriptions", "places.businessStatus",
        "nextPageToken",
    ])
    status, body = _http(
        "https://places.googleapis.com/v1/places:searchText",
        data=payload, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": field_mask,
        },
    )
    data = json.loads(body)
    places = data.get("places", [])
    report["places_api"] = {
        "ok": True,
        "http_status": status,
        "returned": len(places),
        "has_next_page_token": bool(data.get("nextPageToken")),
        "sample": [
            {
                "name": (p.get("displayName") or {}).get("text"),
                "website": p.get("websiteUri"),
                "primary_type": p.get("primaryType"),
                "rating": p.get("rating"),
                "price_level": p.get("priceLevel"),
            }
            for p in places[:5]
        ],
    }
except urllib.error.HTTPError as e:
    report["places_api"] = {"ok": False, "http_status": e.code, "error": e.read().decode()[:800]}
except Exception as e:
    report["places_api"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

# COMMAND ----------
# 3) Region (best effort; may be unavailable on serverless)
region = "unknown"
for key in ("spark.databricks.clusterUsageTags.region",
            "spark.databricks.clusterUsageTags.dataPlaneRegion",
            "spark.databricks.workspaceUrl"):
    try:
        v = spark.conf.get(key)
        if v:
            region = f"{key}={v}"
            break
    except Exception:
        continue
report["region_hint"] = region

# COMMAND ----------
# 4) ai_query against a foundation model
try:
    row = spark.sql(
        "SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', "
        "'Reply with only the word: ok') AS out"
    ).collect()[0]
    report["ai_query"] = {"ok": True, "sample": str(row["out"])[:60]}
except Exception as e:
    report["ai_query"] = {"ok": False, "error": f"{type(e).__name__}: {str(e)[:400]}"}

# COMMAND ----------
# 5) ai_parse_document resolves?
try:
    desc = spark.sql("DESCRIBE FUNCTION ai_parse_document").collect()
    report["ai_parse_document"] = {"resolves": True, "rows": len(desc)}
except Exception as e:
    report["ai_parse_document"] = {"resolves": False, "error": f"{type(e).__name__}: {str(e)[:400]}"}

# COMMAND ----------
print(json.dumps(report, indent=2, default=str))
dbutils.notebook.exit(json.dumps(report, default=str))
