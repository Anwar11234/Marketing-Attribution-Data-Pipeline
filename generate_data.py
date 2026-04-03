"""
Marketing Attribution Pipeline - Source Data Generator
======================================================
Generates realistic, messy raw data for 3 source systems:
  1. Google Ads    → CSV files  (daily spend exports)
  2. Segment       → JSONL file (nested clickstream events)
  3. Postgres      → SQL file   (conversion INSERT statements)

Intentional messiness included (matching Tim's articles):
  - Google Ads: inconsistent date formats, duplicate rows, trailing
                whitespace in campaign names, mixed-case channels
  - Segment:    nested JSON payloads, missing fields on older events,
                inconsistent null representations, schema drift mid-year
  - Postgres:   snake_case naming from backend, integer IDs, UTC offset
                timezone strings instead of proper timestamps

Usage:
  python generate_data.py

Re-run anytime to regenerate data up to today's date.
Output lands in ./raw_data/
"""

import csv
import json
import os
import random
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Seed & date range ────────────────────────────────────────────────────────

random.seed(42)  # reproducible randomness; change to reseed differently

END_DATE   = date.today()
START_DATE = END_DATE - timedelta(days=365)

# ── Output dirs ──────────────────────────────────────────────────────────────

RAW_DIR      = Path("raw_data")
GOOGLE_DIR   = RAW_DIR / "google_ads"
SEGMENT_DIR  = RAW_DIR / "segment"
POSTGRES_DIR = RAW_DIR / "postgres"

for d in [GOOGLE_DIR, SEGMENT_DIR, POSTGRES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Master reference data ────────────────────────────────────────────────────

CAMPAIGNS = [
    # (id, name, platform, channel, budget_usd/day)
    (1,  "Brand Awareness Q1",          "google",   "search",   450),
    (2,  "Summer Sale - Retargeting",   "google",   "display",  320),
    (3,  "Product Launch - Video",      "google",   "video",    600),
    (4,  "Holiday Promo",               "google",   "shopping", 800),
    (5,  "New User Acquisition",        "google",   "search",   550),
    (6,  "Brand Awareness Q1",          "meta",     "search",   400),   # same name, different platform
    (7,  "Summer Sale Carousel",        "meta",     "display",  290),
    (8,  "Retargeting - Cart Abandon",  "meta",     "display",  180),
    (9,  "Holiday Promo",               "meta",     "shopping", 750),
    (10, "Lookalike Expansion",         "meta",     "search",   420),
    (11, "Brand Awareness Q1",          "linkedin", "search",   300),
    (12, "B2B Lead Gen",                "linkedin", "display",  500),
    (13, "Thought Leadership",          "linkedin", "video",    380),
    (14, "Event Promotion",             "linkedin", "display",  260),
    (15, "Product Demo Offer",          "linkedin", "search",   340),
    (16, "Competitor Conquest",         "google",   "search",   210),
    (17, "App Install Campaign",        "google",   "display",  490),
    (18, "Podcast Sponsorship",         "meta",     "video",    650),
    (19, "Influencer Amplification",    "meta",     "video",    420),
    (20, "Career Page Visitors",        "linkedin", "display",  190),
    (21, "Dynamic Search Ads",          "google",   "search",   370),
    (22, "YouTube Bumpers",             "google",   "video",    280),
    (23, "Spring Collection",           "meta",     "shopping", 620),
    (24, "Back to School",              "google",   "shopping", 710),
    (25, "Flash Sale - Email Lookalike","meta",     "search",   330),
]

# Campaign active windows (some campaigns only run part of the year)
CAMPAIGN_WINDOWS = {
    1:  (START_DATE,               START_DATE + timedelta(days=90)),
    2:  (START_DATE + timedelta(days=60),  START_DATE + timedelta(days=210)),
    3:  (START_DATE + timedelta(days=30),  START_DATE + timedelta(days=120)),
    4:  (END_DATE   - timedelta(days=60),  END_DATE),
    5:  (START_DATE,               END_DATE),
    6:  (START_DATE,               START_DATE + timedelta(days=90)),
    7:  (START_DATE + timedelta(days=60),  START_DATE + timedelta(days=210)),
    8:  (START_DATE,               END_DATE),
    9:  (END_DATE   - timedelta(days=60),  END_DATE),
    10: (START_DATE + timedelta(days=180), END_DATE),
    11: (START_DATE,               START_DATE + timedelta(days=90)),
    12: (START_DATE,               END_DATE),
    13: (START_DATE + timedelta(days=90),  END_DATE),
    14: (START_DATE + timedelta(days=150), START_DATE + timedelta(days=270)),
    15: (START_DATE,               END_DATE),
    16: (START_DATE + timedelta(days=30),  END_DATE),
    17: (START_DATE + timedelta(days=45),  START_DATE + timedelta(days=300)),
    18: (START_DATE + timedelta(days=120), END_DATE),
    19: (START_DATE + timedelta(days=180), END_DATE),
    20: (START_DATE,               END_DATE),
    21: (START_DATE,               END_DATE),
    22: (START_DATE + timedelta(days=30),  START_DATE + timedelta(days=250)),
    23: (START_DATE + timedelta(days=60),  START_DATE + timedelta(days=180)),
    24: (START_DATE + timedelta(days=150), START_DATE + timedelta(days=240)),
    25: (START_DATE + timedelta(days=200), END_DATE),
}

USER_IDS = [f"u_{i:05d}" for i in range(1, 5001)]  # 5000 fake users

EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "search", "product_view"]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def campaign_active_on(cid: int, d: date) -> bool:
    start, end = CAMPAIGN_WINDOWS.get(cid, (START_DATE, END_DATE))
    return start <= d <= end


def jitter(base: float, pct: float = 0.25) -> float:
    """Return base ± pct random noise."""
    return round(base * random.uniform(1 - pct, 1 + pct), 2)


def surge_multiplier(d: date) -> float:
    """Higher spend around Black Friday / holiday season."""
    days_to_year_end = (date(d.year, 12, 31) - d).days
    if 20 <= days_to_year_end <= 60:   # Nov–Dec
        return random.uniform(1.4, 2.2)
    if d.month in (7, 8):              # summer
        return random.uniform(1.1, 1.4)
    return 1.0


# ─────────────────────────────────────────────────────────────────────────────
# 1. GOOGLE ADS  →  CSV
# ─────────────────────────────────────────────────────────────────────────────
# Messiness injected:
#   • date column uses 3 different string formats (randomly chosen per row)
#   • campaign names sometimes have trailing/leading whitespace
#   • channel values are inconsistent (e.g. "Paid Search", "paid_search", "SEARCH")
#   • ~3 % of rows are duplicates (soft deletes included)
#   • soft_deleted flag exists but is easy to miss

GOOGLE_DATE_FMTS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"]   # intentional inconsistency

GOOGLE_CHANNEL_VARIANTS = {
    "search":   ["search", "Paid Search", "paid_search", "SEARCH", "Search"],
    "display":  ["display", "Display", "DISPLAY", "display_network"],
    "video":    ["video", "Video", "VIDEO", "youtube_video"],
    "shopping": ["shopping", "Shopping", "SHOPPING", "product_shopping"],
}


def messy_date(d: date) -> str:
    fmt = random.choice(GOOGLE_DATE_FMTS)
    return d.strftime(fmt)


def messy_campaign_name(name: str) -> str:
    if random.random() < 0.12:
        name = name + "  "          # trailing whitespace
    if random.random() < 0.05:
        name = "  " + name          # leading whitespace
    return name


def messy_channel(channel: str) -> str:
    return random.choice(GOOGLE_CHANNEL_VARIANTS[channel])


print("Generating Google Ads CSVs …")

google_campaigns = [c for c in CAMPAIGNS if c[2] == "google"]

for d in daterange(START_DATE, END_DATE):
    rows = []
    for cid, name, platform, channel, budget in google_campaigns:
        if not campaign_active_on(cid, d):
            continue

        mult  = surge_multiplier(d)
        spend = jitter(budget * mult * random.uniform(0.5, 1.0))
        impr  = int(spend * random.uniform(80, 200))
        clicks= int(impr * random.uniform(0.01, 0.08))

        row = {
            "date":           messy_date(d),
            "campaign_id":    str(cid),
            "campaign_name":  messy_campaign_name(name),
            "channel":        messy_channel(channel),
            "spend_usd":      spend,
            "impressions":    impr,
            "clicks":         clicks,
            "soft_deleted":   "false",
        }
        rows.append(row)

        # ~3 % duplicate rows (soft deleted)
        if random.random() < 0.03:
            dup = dict(row)
            dup["soft_deleted"] = "true"
            rows.append(dup)

    if not rows:
        continue

    fname = GOOGLE_DIR / f"google_ads_{d.strftime('%Y%m%d')}.csv"
    with open(fname, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

print(f"  → {len(list(GOOGLE_DIR.iterdir()))} CSV files written to {GOOGLE_DIR}/")


# ─────────────────────────────────────────────────────────────────────────────
# 2. SEGMENT CLICKSTREAM  →  JSONL
# ─────────────────────────────────────────────────────────────────────────────
# Messiness injected:
#   • nested `properties` JSON payload (varies by event type)
#   • timestamps as UTC strings, but format changes mid-year (schema drift)
#   • events before March 2023 equivalent (first 90 days) missing `session_id`
#   • some rows use "", "N/A", or "null" string for missing values
#   • ~1 % of events have a completely missing `user_id` (anonymous)
#   • `context` block structure changes after day 180

SCHEMA_DRIFT_DAY = START_DATE + timedelta(days=180)
MISSING_SESSION_CUTOFF = START_DATE + timedelta(days=90)

NULL_VARIANTS = ["", "N/A", "null", None]   # inconsistent nulls


def segment_timestamp(d: date) -> str:
    """Format changes after SCHEMA_DRIFT_DAY."""
    dt = datetime(d.year, d.month, d.day,
                  random.randint(0, 23),
                  random.randint(0, 59),
                  random.randint(0, 59))
    if d >= SCHEMA_DRIFT_DAY:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")          # ISO 8601
    else:
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")        # old format


def build_properties(event_type: str, cid: int, channel: str) -> dict:
    base = {
        "campaign_id": str(cid),
        "channel":     channel,
        "page_url":    f"https://example.com/{random.choice(['home','products','about','pricing'])}",
    }
    if event_type == "add_to_cart":
        base["product_id"]  = f"prod_{random.randint(1000,9999)}"
        base["product_name"]= random.choice(["Widget Pro","Gadget X","Thing Plus","Doohickey"])
        base["price_usd"]   = round(random.uniform(9.99, 299.99), 2)
        base["quantity"]    = random.randint(1, 5)
    elif event_type == "checkout_start":
        base["cart_total"]  = round(random.uniform(20, 800), 2)
        base["item_count"]  = random.randint(1, 8)
    elif event_type == "search":
        base["query"]       = random.choice(["widget","best price","review","compare","discount"])
        base["results_count"]= random.randint(0, 50)
    elif event_type == "product_view":
        base["product_id"]  = f"prod_{random.randint(1000,9999)}"
        base["time_on_page"]= random.randint(5, 300)
    return base


def build_context(d: date) -> dict:
    """Context block structure changes after SCHEMA_DRIFT_DAY."""
    if d >= SCHEMA_DRIFT_DAY:
        return {
            "library": {"name": "analytics.js", "version": "2.11.1"},
            "userAgent": "Mozilla/5.0 (compatible)",
            "ip":         f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
            "locale":     random.choice(["en-US","en-GB","fr-FR","de-DE"]),
        }
    else:
        # older, flatter context
        return {
            "library_name":    "analytics.js",
            "library_version": "2.9.0",
            "user_agent":      "Mozilla/5.0",
        }


print("Generating Segment JSONL …")

all_google_meta_campaigns = [c for c in CAMPAIGNS]  # all platforms drive clicks
events_written = 0

with open(SEGMENT_DIR / "segment_tracks.jsonl", "w") as f:
    for d in daterange(START_DATE, END_DATE):
        # number of events that day scales with active campaigns
        active = [c for c in all_google_meta_campaigns if campaign_active_on(c[0], d)]
        n_events = int(len(active) * random.uniform(30, 120) * surge_multiplier(d))

        for _ in range(n_events):
            cid, name, platform, channel, _ = random.choice(active) if active else CAMPAIGNS[0]
            uid = random.choice(USER_IDS)
            event_type = random.choice(EVENT_TYPES)

            event = {
                "message_id":  hashlib.md5(f"{d}{uid}{events_written}".encode()).hexdigest(),
                "type":        "track",
                "event":       event_type,
                "timestamp":   segment_timestamp(d),
                "user_id":     None if random.random() < 0.01 else uid,   # ~1% anonymous
                "anonymous_id":f"anon_{random.randint(100000,999999)}",
                "properties":  build_properties(event_type, cid, channel),
                "context":     build_context(d),
            }

            # inject missing session_id for early dates
            if d >= MISSING_SESSION_CUTOFF:
                event["session_id"] = f"sess_{random.randint(10000000,99999999)}"
            # else: field simply absent

            # random null variants for optional fields
            if random.random() < 0.04:
                event["user_id"] = random.choice(NULL_VARIANTS)

            f.write(json.dumps(event) + "\n")
            events_written += 1

print(f"  → {events_written:,} events written to {SEGMENT_DIR}/segment_tracks.jsonl")


# ─────────────────────────────────────────────────────────────────────────────
# 3. POSTGRES BACKEND  →  SQL (INSERT statements)
# ─────────────────────────────────────────────────────────────────────────────
# Messiness injected:
#   • column names follow backend snake_case, not data-team conventions
#   • `user_id` is INTEGER in source (must be cast to string in warehouse)
#   • timestamps are stored as strings with "+00:00" offset, not proper TIMESTAMPTZ
#   • `conversion_type` uses numeric codes (not human-readable labels)
#   • some rows have NULL revenue (free-tier signups)
#   • `created_at` vs `converted_at` naming inconsistency in older rows

CONVERSION_TYPES = {1: "purchase", 2: "signup", 3: "trial_start", 4: "demo_request"}

print("Generating Postgres SQL …")

conversions_written = 0
active_all = [c for c in CAMPAIGNS]

with open(POSTGRES_DIR / "conversions.sql", "w") as f:
    f.write("-- Conversions export from app Postgres backend\n")
    f.write("-- Exported: {}\n\n".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")))
    f.write(
        "CREATE TABLE IF NOT EXISTS app_conversions (\n"
        "    conversion_id   INTEGER PRIMARY KEY,\n"
        "    usr_id          INTEGER,\n"          # ← intentional backend naming
        "    cmpgn_id        INTEGER,\n"          # ← abbreviated, non-standard
        "    conv_type_cd    INTEGER,\n"          # ← numeric code, not label
        "    revenue_amt     NUMERIC(10,2),\n"    # ← NULL for free signups
        "    conv_ts         VARCHAR(32),\n"      # ← stored as string with TZ offset
        "    created_at      VARCHAR(32)\n"       # ← duplicate/overlapping timestamp
        ");\n\n"
    )

    conversion_id = 1

    for d in daterange(START_DATE, END_DATE):
        active = [c for c in active_all if campaign_active_on(c[0], d)]
        if not active:
            continue

        # Conversions are rarer than clicks — roughly 0.5–3 % of clicks
        n_conversions = int(len(active) * random.uniform(0.5, 4) * surge_multiplier(d))

        for _ in range(n_conversions):
            cid, name, platform, channel, budget = random.choice(active)
            uid_int  = random.randint(1, 5000)
            conv_type= random.choices([1, 2, 3, 4], weights=[40, 30, 20, 10])[0]

            # Revenue: NULL for signups/trials, positive for purchases, variable for demos
            if conv_type == 1:        # purchase
                revenue = round(random.uniform(9.99, 499.99), 2)
            elif conv_type == 4:      # demo → pipeline value
                revenue = round(random.uniform(500, 5000), 2)
            else:                     # signup / trial → NULL
                revenue = None

            # Timestamp as string with offset (messy)
            dt = datetime(d.year, d.month, d.day,
                          random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
            conv_ts   = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            created_at= dt.strftime("%Y-%m-%dT%H:%M:%SZ")   # different format same value

            revenue_sql = "NULL" if revenue is None else str(revenue)

            f.write(
                f"INSERT INTO app_conversions VALUES "
                f"({conversion_id}, {uid_int}, {cid}, {conv_type}, "
                f"{revenue_sql}, '{conv_ts}', '{created_at}');\n"
            )

            conversion_id  += 1
            conversions_written += 1

print(f"  → {conversions_written:,} rows written to {POSTGRES_DIR}/conversions.sql")


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

google_files = len(list(GOOGLE_DIR.iterdir()))
print("\n─────────────────────────────────────────────────")
print("  Data generation complete!")
print(f"  Date range : {START_DATE} → {END_DATE}")
print(f"  Campaigns  : {len(CAMPAIGNS)}")
print(f"  Google Ads : {google_files} CSV files          → {GOOGLE_DIR}/")
print(f"  Segment    : {events_written:,} events (JSONL) → {SEGMENT_DIR}/segment_tracks.jsonl")
print(f"  Postgres   : {conversions_written:,} conversions (SQL) → {POSTGRES_DIR}/conversions.sql")
print("─────────────────────────────────────────────────")
print("\nMessiness summary (what your Bronze layer will need to handle):")
print("  Google Ads : 3 date formats, channel naming variants, trailing whitespace,")
print("               soft-deleted duplicate rows (~3%)")
print("  Segment    : nested JSON properties, 2 timestamp formats (schema drift at day 180),")
print("               missing session_id before day 90, inconsistent nulls ('', 'N/A', 'null')")
print("  Postgres   : abbreviated column names (usr_id, cmpgn_id, conv_type_cd),")
print("               numeric type codes, revenue NULLs, timestamps as strings with TZ offset")