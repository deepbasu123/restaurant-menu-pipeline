# Restaurant Menu Pipeline for Databricks

A Databricks notebook that finds restaurants with the Google Places API, visits each
restaurant's own website to locate its menu, extracts structured menu items with Databricks
AI functions, and writes everything to Unity Catalog as Delta tables.

It runs on serverless compute, uses only the Python standard library (no `pip install`), and
reads your Google API key from a Databricks secret so nothing sensitive ends up in the
notebook or in git.

## What it does

```
Google Places API           each restaurant's own site         Databricks AI              Unity Catalog
──────────────────          ──────────────────────────         ─────────────             ──────────────
find restaurants     ──▶     fetch homepage, find the     ──▶   ai_parse_document   ──▶    restaurants
+ metadata + website         menu page or PDF (robots.txt        + ai_query                menu_documents
                             respected)                          → structured items        menu_items
```

## What you get in Unity Catalog

Everything lands under `<catalog>.<schema>` (default `restaurant_menus`):

| Object | Layer | Contents |
|---|---|---|
| `raw` (Volume) | raw | The fetched HTML and PDF bytes, kept for provenance |
| `restaurants_bronze` | bronze | The raw Places API JSON, one row per place |
| `restaurants_silver` | silver | Cleaned metadata, one row per place (staging for `restaurants`) |
| `menu_documents` | silver | Every fetched source and how the fetch went |
| `menu_items` | gold | The structured line items: section, name, description, price, currency |
| `restaurants` | gold | Metadata plus `menu_status` and `menu_item_count`. This is the table to query. |

## The honest part

Every restaurant gets a metadata row. Not every restaurant gives you a menu. Menus in the
wild live as HTML, PDFs, images, or inside JavaScript ordering widgets like Mr Yum, Bopple,
and the delivery apps. The pipeline reads HTML and PDF menus well, images passably through
`ai_parse_document`, and the JavaScript widgets barely (a plain fetch never sees text a
browser renders with JavaScript).

On a real 200-restaurant run across South East QLD, 56 restaurants (about 1 in 4) yielded a
full structured menu, for 2,355 line items. The rest broke down as: 101 sites returned a page
but no parseable menu (mostly JavaScript ordering widgets), 25 blocked us in `robots.txt`, 7
rate-limited or refused the request, 7 had no menu link, and 4 had no website at all.

Those figures predate a fix to how menu links are picked off a homepage: the HTML parser used
to attribute every block of text after a link to that link, which sent the crawler after the
wrong pages and burned the per-site candidate budget. On a 15-restaurant sample, fixing it took
the yield from 5 restaurants to 9, and several sites that previously fell back to their homepage
now return their real menu page. The 200-restaurant numbers above have not been re-measured
since, so treat them as a floor rather than a current benchmark. The `menu_status` column tells
you exactly what happened for each restaurant:

| `menu_status` | Meaning |
|---|---|
| `menu_extracted` | We found a menu and pulled structured items |
| `source_found_no_items` | We fetched a page but nothing that parsed as a menu (often a JS ordering widget) |
| `robots_blocked` | The site's `robots.txt` told us not to crawl it, so we didn't |
| `fetch_failed` | The site errored or timed out |
| `no_website` | Places had no website for this restaurant |
| `no_menu_found` | Fetched, but no menu link and the homepage was too thin |
| `not_attempted` | The restaurant has a website but we never tried it, because it fell outside `max_sites_to_fetch`. Raise that cap to include it. |

The extraction prompt is written to pull only items that are actually on the page. Where a
price is not shown, it stores `null` rather than guessing.

## Brand new to Databricks? Start here

This is the from-zero walkthrough. If you already use Databricks, skip to [Setup](#setup).

### Step 1. Get a Databricks workspace
You need a workspace with serverless compute, Unity Catalog, and the Foundation Model APIs.
- If your company uses Databricks, ask for access and your workspace URL. It looks like
  `https://something.cloud.databricks.com` (AWS) or `https://adb-xxxx.azuredatabricks.net` (Azure).
- If you don't have one, sign up at https://www.databricks.com/try-databricks. There is a free
  option that runs on serverless with Unity Catalog.
- Log in and keep the browser tab open. Step 7 tells you whether your workspace supports the AI
  functions, so you don't have to guess now.

### Step 2. Get a Google Places API key
The pipeline finds restaurants through Google, so you need a key.
1. Go to https://console.cloud.google.com and sign in with a Google account.
2. In the top bar, open the project dropdown and click "New Project". Name it (for example
   `restaurant-menus`), create it, and make sure it is selected.
3. Turn on billing: left menu, Billing, link a billing account. A card is required. Google gives
   a recurring monthly credit that usually covers a run this size.
4. Enable the API: left menu, APIs & Services, Library, search "Places API (New)", open it, Enable.
5. Create the key: APIs & Services, Credentials, "Create credentials", "API key". Copy the key.
6. Recommended: click the key and, under API restrictions, limit it to the Places API, then save.

Treat the key like a password. You will paste it once in Step 5 and never put it in the notebook.

### Step 3. Install the Databricks CLI (one time)
You need this once, to store your key as a secret. There is no button in the UI for creating
secrets, so this is the standard way.
- Mac or Linux, in a terminal:
  ```bash
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
  ```
- Windows: follow the short official installer guide at
  https://docs.databricks.com/dev-tools/cli/install.html
- Check it worked:
  ```bash
  databricks -v
  ```

### Step 4. Log in to your workspace from the CLI
```bash
databricks auth login --host https://YOUR-WORKSPACE-URL
```
Use your URL from Step 1. A browser opens for sign-in. When it says the profile was saved, you
are connected.

### Step 5. Store your key as a secret
Run these two commands. The second prompts you to paste the key, then press Enter:
```bash
databricks secrets create-scope restaurant_pipeline
databricks secrets put-secret restaurant_pipeline google_places_api_key
```
The key now lives in Databricks, not in the notebook or in this repo.

### Step 6. Get the notebooks into your workspace
In the Databricks browser tab: left sidebar, Workspace, open your home folder, click the menu
next to it and choose Import, then drag in `restaurant_menu_pipeline.py` and `00_stage0_probe.py`
from this repo. They import as notebooks. (CLI alternative:
`databricks sync . /Users/YOUR-EMAIL/restaurant-menu-pipeline`.)

### Step 7. Check your workspace can do the job
1. Open `00_stage0_probe`.
2. Top right, set the compute to Serverless.
3. Click "Run all" and wait about a minute.
4. Read the printed report. You want `ok: true` for egress and the Places API, and
   `resolves: true` for `ai_parse_document`. If egress is false, your workspace blocks internet
   access and you should ask your admin to allow it.

### Step 8. Run the pipeline
1. Open `restaurant_menu_pipeline` and set compute to Serverless.
2. Widgets appear across the top. The defaults gather 200 restaurants around Brisbane. Change
   `search_queries` for a different area, `target_count` for a different number, and
   `default_currency` to match the region.
3. Click "Run all". A 200-restaurant run takes roughly 10 to 15 minutes.
4. The last cells show a summary and a sample of extracted menus.

### Step 9. See your data
Left sidebar, Catalog, open your catalog, then your schema (`restaurant_menus`). Click the
`restaurants` table for the overview, or open a SQL editor and run the queries in
[Querying the results](#querying-the-results).

## Prerequisites

1. A Databricks workspace with Unity Catalog, serverless compute, and the Foundation Model
   APIs available (any recent workspace has these).
2. Serverless internet egress enabled, so the notebook can reach Google and the restaurant
   sites. Run `00_stage0_probe` first to confirm.
3. A Google Cloud project with the **Places API (New)** enabled and billing turned on, and an
   API key for it. See [Google's setup guide](https://developers.google.com/maps/documentation/places/web-service/get-api-key).

## Setup

### 1. Store your Google API key as a Databricks secret

Never paste the key into the notebook. Put it in a secret scope instead:

```bash
databricks secrets create-scope restaurant_pipeline
databricks secrets put-secret restaurant_pipeline google_places_api_key
# paste the key when prompted, or pipe it from a file via stdin
```

The notebook reads it with `dbutils.secrets.get(...)`, which Databricks redacts in any
output.

### 2. Get the notebooks into your workspace

```bash
databricks sync . /Users/<you>@example.com/restaurant-menu-pipeline
```

Or import the two `.py` files through the workspace UI (they are notebook source files).

### 3. Confirm your workspace can do the job

Run `00_stage0_probe`. It checks four things and prints a report: internet egress, the Places
API answering with your key, `ai_query`, and `ai_parse_document`. If egress is blocked, sort
that out before running the main notebook.

### 4. Run the pipeline

Open `restaurant_menu_pipeline` and set the widgets, then run all. It writes the tables and
prints a summary at the end.

## Parameters

| Widget | Default | What it controls |
|---|---|---|
| `catalog` | `deep_test_1_catalog` | Target Unity Catalog catalog (must already exist) |
| `schema` | `restaurant_menus` | Target schema, created if missing |
| `secret_scope` | `restaurant_pipeline` | Secret scope holding the key |
| `secret_key` | `google_places_api_key` | Secret key name |
| `search_queries` | South East QLD suburbs | Comma-separated Places text queries |
| `target_count` | `200` | How many unique restaurants to gather |
| `max_sites_to_fetch` | `200` | Cap on sites fetched, to bound time and cost. Restaurants past the cap are marked `not_attempted`. |
| `extraction_model` | `databricks-meta-llama-3-3-70b-instruct` | The model `ai_query` uses to extract menus |
| `default_currency` | `AUD` | Currency stamped on every price (one region per run) |
| `reset_tables` | `false` | Drop and rebuild the tables instead of overwriting |

Currency is set from `default_currency`, not inferred by the model, because a model asked to
guess currency from menu text sometimes gets it wrong. Run one region at a time and set this
to match.

To point it at a different area, change `search_queries`. For example, Melbourne cafes:
`cafes in Fitzroy VIC, cafes in Carlton VIC, cafes in Brunswick VIC`.

## Querying the results

```sql
-- Coverage overview
SELECT menu_status, COUNT(*) AS restaurants, SUM(menu_item_count) AS items
FROM restaurant_menus.restaurants
GROUP BY menu_status ORDER BY restaurants DESC;

-- Menus with prices
SELECT r.name, i.section, i.item_name, i.price, i.currency
FROM restaurant_menus.menu_items i
JOIN restaurant_menus.restaurants r USING (place_id)
WHERE i.price IS NOT NULL
ORDER BY r.name, i.section;
```

## Cost

- **Places API**: billed per request, and the amount depends on which fields you ask for.
  Gathering a couple of hundred restaurants is a few dollars, and Google's recurring monthly
  credit may cover it. The notebook uses pagination and dedupes, so it makes far fewer
  requests than the restaurant count.
- **AI functions**: `ai_query` and `ai_parse_document` are pay-per-token on Foundation Model
  APIs. Extraction runs once per fetched menu.

## Conduct and data use

The pipeline reads each restaurant's own website, not Google's pages, and it honours
`robots.txt`, sends a descriptive User-Agent, and spaces out its requests. Using the extracted
menus for your own analysis is the defensible zone. Redistributing scraped menu content is a
separate question worth checking before you do it.

## Running it on a schedule

This version is built to run interactively first so you can see the output. To refresh on a
schedule, wrap the notebook in a Lakeflow Job (Workflows) with a cron trigger. Switch the
table writes from overwrite to a `MERGE` on `place_id` if you want reruns to update rather
than replace.

## Repository layout

```
restaurant_menu_pipeline.py   the main pipeline notebook
00_stage0_probe.py            feasibility probe, run this first
README.md                     this file
.gitignore                    keeps secrets and local state out of git
```

## Troubleshooting

- **Egress blocked in the probe**: your workspace restricts serverless outbound traffic. Use
  classic compute on a VPC with a NAT gateway, or run discovery outside Databricks and load
  the results in.
- **Lots of `source_found_no_items`**: those sites serve their menu through a JavaScript
  ordering widget that a plain fetch cannot see. That is expected, not a bug.
- **Lots of `robots_blocked`**: some site builders ship a restrictive `robots.txt`. The
  pipeline respects it on purpose.
- **`ai_parse_document` errors**: check your workspace region supports it; the pipeline skips
  PDF parsing and carries on with HTML if it is unavailable.
- **Some item names run together** (like `AKAVODKABERRY`): a few heavily styled menus put each
  word in its own HTML element with no spacing, and the plain-text extraction joins them. It
  is rare and concentrated in fancy cocktail or sake lists. Fixing it properly needs a
  browser-rendering fetch, which this version avoids to stay dependency-free.
- **`fetch_failed` with 429 or 403**: the site rate-limited or blocked the request. A slower
  crawl (raise the delay) helps with 429s; 403s are usually bot protection.
