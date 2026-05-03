"""
Marketing Attribution Pipeline - Source Data Generator
======================================================
Generates realistic, messy raw data for 5 source systems:
  1. Google Ads   → CSV files  (daily spend exports)
  2. Meta Ads     → CSV files  (daily spend exports)
  3. LinkedIn Ads → CSV files  (daily spend exports)
  4. Segment      → JSONL file (nested clickstream events)
  5. Postgres     → SQL file   (conversion INSERT statements)

Intentional messiness per source:
  - Google Ads:   inconsistent date formats, duplicate rows with soft_deleted flag,
                  trailing/leading whitespace in campaign names, mixed-case channel variants,
                  campaign_status as lowercase strings ("enabled"/"paused"/"removed")
  - Meta Ads:     different column names (report_date, amount_spent, reach, link_clicks,
                  placement_type), Meta-specific channel taxonomy, is_deleted as Python
                  bool strings ("True"/"False"), occasional missing currency field,
                  effective_status as uppercase strings ("ACTIVE"/"PAUSED"/"DELETED")
  - LinkedIn Ads: different column names (start_date, cost_in_usd, total_clicks, ad_type),
                  LinkedIn-specific channel taxonomy (Sponsored Content, Text Ads, etc.),
                  campaign_status field, date format includes human-readable variant
  - Segment:      nested JSON payloads, missing fields on older events,
                  inconsistent null representations, schema drift mid-year
  - Postgres:     snake_case naming from backend, integer IDs, UTC offset
                  timezone strings instead of proper timestamps

Incremental behaviour:
  - First run       → generates a full year of historical data
  - Subsequent runs → detects the last generated date per source
                      and appends only the new days up to today
  - Safe to run daily via cron / Task Scheduler

Usage:
  python generate_data.py

Output lands in ./raw_data/
"""

import csv
import json
import random
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Seed & dates ─────────────────────────────────────────────────────────────

random.seed(42)

TODAY      = date.today()
FIRST_DATE = TODAY - timedelta(days=365)   # used only on a fresh run

# ── Output dirs ──────────────────────────────────────────────────────────────

RAW_DIR       = Path("raw_data")
GOOGLE_DIR    = RAW_DIR / "google_ads"
META_DIR      = RAW_DIR / "meta_ads"
LINKEDIN_DIR  = RAW_DIR / "linkedin_ads"
SEGMENT_DIR   = RAW_DIR / "segment"
POSTGRES_DIR  = RAW_DIR / "postgres"

for d in [GOOGLE_DIR, META_DIR, LINKEDIN_DIR, SEGMENT_DIR, POSTGRES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Master reference data ────────────────────────────────────────────────────

CAMPAIGNS = [
    # (id, name, platform, channel, budget_usd/day)
    (1,  "Brand Awareness Q1",           "google",   "search",   450),
    (2,  "Summer Sale - Retargeting",    "google",   "display",  320),
    (3,  "Product Launch - Video",       "google",   "video",    600),
    (4,  "Holiday Promo",                "google",   "shopping", 800),
    (5,  "New User Acquisition",         "google",   "search",   550),
    (6,  "Brand Awareness Q1",           "meta",     "search",   400),
    (7,  "Summer Sale Carousel",         "meta",     "display",  290),
    (8,  "Retargeting - Cart Abandon",   "meta",     "display",  180),
    (9,  "Holiday Promo",                "meta",     "shopping", 750),
    (10, "Lookalike Expansion",          "meta",     "search",   420),
    (11, "Brand Awareness Q1",           "linkedin", "search",   300),
    (12, "B2B Lead Gen",                 "linkedin", "display",  500),
    (13, "Thought Leadership",           "linkedin", "video",    380),
    (14, "Event Promotion",              "linkedin", "display",  260),
    (15, "Product Demo Offer",           "linkedin", "search",   340),
    (16, "Competitor Conquest",          "google",   "search",   210),
    (17, "App Install Campaign",         "google",   "display",  490),
    (18, "Podcast Sponsorship",          "meta",     "video",    650),
    (19, "Influencer Amplification",     "meta",     "video",    420),
    (20, "Career Page Visitors",         "linkedin", "display",  190),
    (21, "Dynamic Search Ads",           "google",   "search",   370),
    (22, "YouTube Bumpers",              "google",   "video",    280),
    (23, "Spring Collection",            "meta",     "shopping", 620),
    (24, "Back to School",               "google",   "shopping", 710),
    (25, "Flash Sale - Email Lookalike", "meta",     "search",   330),
]

def build_campaign_windows(first: date, last: date) -> dict:
    return {
        1:  (first,                       first + timedelta(days=90)),
        2:  (first + timedelta(days=60),  first + timedelta(days=210)),
        3:  (first + timedelta(days=30),  first + timedelta(days=120)),
        4:  (last  - timedelta(days=60),  last),
        5:  (first,                       last),
        6:  (first,                       first + timedelta(days=90)),
        7:  (first + timedelta(days=60),  first + timedelta(days=210)),
        8:  (first,                       last),
        9:  (last  - timedelta(days=60),  last),
        10: (first + timedelta(days=180), last),
        11: (first,                       first + timedelta(days=90)),
        12: (first,                       last),
        13: (first + timedelta(days=90),  last),
        14: (first + timedelta(days=150), first + timedelta(days=270)),
        15: (first,                       last),
        16: (first + timedelta(days=30),  last),
        17: (first + timedelta(days=45),  first + timedelta(days=300)),
        18: (first + timedelta(days=120), last),
        19: (first + timedelta(days=180), last),
        20: (first,                       last),
        21: (first,                       last),
        22: (first + timedelta(days=30),  first + timedelta(days=250)),
        23: (first + timedelta(days=60),  first + timedelta(days=180)),
        24: (first + timedelta(days=150), first + timedelta(days=240)),
        25: (first + timedelta(days=200), last),
    }

# 5000 users shared across both Segment and Postgres.
# Segment stores them as prefixed strings: "u_00001" ... "u_05000"
# Postgres stores them as plain integers:   1 ... 5000
USER_IDS     = [f"u_{i:05d}" for i in range(1, 5001)]
USER_ID_INTS = list(range(1, 5001))

EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "search", "product_view"]


# ── Incremental state detection ───────────────────────────────────────────────

def _last_date_from_csv_dir(directory: Path, prefix: str) -> date | None:
    """Return the latest date already generated for a given CSV source dir."""
    files = sorted(directory.glob(f"{prefix}_*.csv"))
    if not files:
        return None
    stem = files[-1].stem.replace(f"{prefix}_", "")   # "20250324"
    return date(int(stem[:4]), int(stem[4:6]), int(stem[6:8]))

def google_last_date()   -> date | None: return _last_date_from_csv_dir(GOOGLE_DIR,   "google_ads")
def meta_last_date()     -> date | None: return _last_date_from_csv_dir(META_DIR,     "meta_ads")
def linkedin_last_date() -> date | None: return _last_date_from_csv_dir(LINKEDIN_DIR, "linkedin_ads")


def segment_last_date() -> date | None:
    seg_file = SEGMENT_DIR / "segment_tracks.jsonl"
    if not seg_file.exists():
        return None
    with open(seg_file, "rb") as f:
        f.seek(0, 2)
        pos   = f.tell()
        lines = []
        while pos > 0 and len(lines) < 2:
            pos = max(0, pos - 4096)
            f.seek(pos)
            lines = f.read().splitlines()
        last_line = lines[-1].decode("utf-8") if lines else ""
    if not last_line.strip():
        return None
    ts = json.loads(last_line).get("timestamp", "")
    return date.fromisoformat(ts[:10])


def postgres_last_date() -> date | None:
    sql_file = POSTGRES_DIR / "conversions.sql"
    if not sql_file.exists():
        return None
    last_ts = None
    with open(sql_file) as f:
        for line in f:
            if line.startswith("INSERT INTO app_conversions VALUES ("):
                try:
                    last_ts = line.split("'")[1][:10]
                except IndexError:
                    pass
    return date.fromisoformat(last_ts) if last_ts else None


def postgres_last_conversion_id() -> int:
    sql_file = POSTGRES_DIR / "conversions.sql"
    if not sql_file.exists():
        return 0
    last_id = 0
    with open(sql_file) as f:
        for line in f:
            if line.startswith("INSERT INTO app_conversions VALUES ("):
                try:
                    last_id = int(line.split("(")[1].split(",")[0].strip())
                except (IndexError, ValueError):
                    pass
    return last_id


def dataset_first_date() -> date:
    files = sorted(GOOGLE_DIR.glob("google_ads_*.csv"))
    if files:
        stem = files[0].stem.replace("google_ads_", "")
        return date(int(stem[:4]), int(stem[4:6]), int(stem[6:8]))
    return FIRST_DATE


# ── General helpers ───────────────────────────────────────────────────────────

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def campaign_active_on(cid: int, d: date, windows: dict) -> bool:
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    return start <= d <= end

def jitter(base: float, pct: float = 0.25) -> float:
    return round(base * random.uniform(1 - pct, 1 + pct), 2)

def surge_multiplier(d: date) -> float:
    days_to_year_end = (date(d.year, 12, 31) - d).days
    if 20 <= days_to_year_end <= 60:
        return random.uniform(1.4, 2.2)
    if d.month in (7, 8):
        return random.uniform(1.1, 1.4)
    return 1.0


# ── Google Ads helpers ────────────────────────────────────────────────────────
# Messiness: inconsistent date formats, mixed-case channel strings,
# trailing/leading whitespace in campaign names, soft-deleted duplicates.

GOOGLE_DATE_FMTS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"]

GOOGLE_CHANNEL_VARIANTS = {
    "search":   ["search", "Paid Search", "paid_search", "SEARCH", "Search"],
    "display":  ["display", "Display", "DISPLAY", "display_network"],
    "video":    ["video", "Video", "VIDEO", "youtube_video"],
    "shopping": ["shopping", "Shopping", "SHOPPING", "product_shopping"],
}

def google_messy_date(d: date) -> str:
    return d.strftime(random.choice(GOOGLE_DATE_FMTS))

def google_messy_campaign_name(name: str) -> str:
    if random.random() < 0.12:
        name = name + "  "
    if random.random() < 0.05:
        name = "  " + name
    return name

def google_messy_channel(channel: str) -> str:
    return random.choice(GOOGLE_CHANNEL_VARIANTS[channel])

# Google uses lowercase status strings
GOOGLE_CAMPAIGN_STATUSES = ["enabled", "paused", "removed"]

def google_campaign_status(cid: int, d: date, windows: dict) -> str:
    """Active campaigns are mostly 'enabled'; ended ones become 'paused' or 'removed'."""
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    if d > end:
        return random.choice(["paused", "removed"])
    return random.choices(["enabled", "paused"], weights=[92, 8])[0]


# ── Meta Ads helpers ──────────────────────────────────────────────────────────
# Messiness: different column names from Google (report_date, amount_spent,
# reach, link_clicks, placement_type), Meta-specific channel taxonomy,
# is_deleted stored as Python-style bool strings ("True"/"False"),
# occasional missing currency field, different date format set.

META_DATE_FMTS = ["%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]

META_CHANNEL_VARIANTS = {
    # Meta calls "search" placements "Paid Social" or "Advantage+ audience"
    "search":   ["Paid Social", "paid_social", "PAID_SOCIAL", "Advantage+ Audience", "advantage_audience"],
    # Meta display = Feed placements
    "display":  ["Feed", "feed", "Instagram Feed", "facebook_feed", "FEED", "Stories"],
    # Meta video = Reels / In-Stream
    "video":    ["Reels", "reels", "In-Stream Video", "REELS", "in_stream_video", "Video Feed"],
    # Meta shopping = Catalog / Dynamic Ads
    "shopping": ["Catalog Sales", "catalog_sales", "CATALOG_SALES", "dynamic_ads", "Shopping"],
}

def meta_messy_date(d: date) -> str:
    return d.strftime(random.choice(META_DATE_FMTS))

def meta_messy_channel(channel: str) -> str:
    return random.choice(META_CHANNEL_VARIANTS[channel])

def meta_messy_campaign_name(name: str) -> str:
    # Meta exports sometimes append the platform suffix
    if random.random() < 0.10:
        name = name + " "
    if random.random() < 0.08:
        name = name + " | Meta"
    return name

# Meta uses uppercase effective_status strings
META_CAMPAIGN_STATUSES = ["ACTIVE", "PAUSED", "DELETED"]

def meta_campaign_status(cid: int, d: date, windows: dict) -> str:
    """Active campaigns are mostly 'ACTIVE'; ended ones become 'PAUSED' or 'DELETED'."""
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    if d > end:
        return random.choice(["PAUSED", "DELETED"])
    return random.choices(["ACTIVE", "PAUSED"], weights=[92, 8])[0]


# ── LinkedIn Ads helpers ──────────────────────────────────────────────────────
# Messiness: different column names (start_date, cost_in_usd, total_clicks,
# ad_type), LinkedIn-specific channel taxonomy (Sponsored Content, Text Ads…),
# campaign_status field with mixed values, human-readable date format variant.

LINKEDIN_DATE_FMTS = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"]

LINKEDIN_CHANNEL_VARIANTS = {
    # LinkedIn "search" = Sponsored Content / Text Ads
    "search":   ["Sponsored Content", "Text Ads", "text_ads", "SPONSORED_CONTENT", "sponsored_content"],
    # LinkedIn "display" = Display Ads / Programmatic
    "display":  ["Display Ads", "display_ads", "DISPLAY", "Programmatic Display", "programmatic_display"],
    # LinkedIn "video" = Sponsored Video / In-Stream
    "video":    ["Video Ads", "video_ads", "SPONSORED_VIDEO", "In-Stream Video", "in_stream_video"],
    # LinkedIn "shopping" = Dynamic Ads / Spotlight
    "shopping": ["Dynamic Ads", "dynamic_ads", "DYNAMIC", "Spotlight Ads", "spotlight_ads"],
}

LINKEDIN_CAMPAIGN_STATUSES = ["ACTIVE", "active", "Active", "PAUSED", "paused", "COMPLETED"]

def linkedin_messy_date(d: date) -> str:
    return d.strftime(random.choice(LINKEDIN_DATE_FMTS))

def linkedin_messy_channel(channel: str) -> str:
    return random.choice(LINKEDIN_CHANNEL_VARIANTS[channel])

def linkedin_messy_campaign_name(name: str) -> str:
    if random.random() < 0.10:
        name = name + " "
    if random.random() < 0.06:
        name = name + " - LinkedIn"
    return name


# ── Segment helpers ───────────────────────────────────────────────────────────

NULL_VARIANTS = ["", "N/A", "null", None]

def segment_timestamp(d: date, schema_drift_day: date) -> str:
    dt = datetime(d.year, d.month, d.day,
                  random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    if d >= schema_drift_day:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

def build_properties(event_type: str, cid: int, channel: str) -> dict:
    base = {
        "campaign_id": str(cid),
        "channel":     channel,
        "page_url":    f"https://example.com/{random.choice(['home','products','about','pricing'])}",
    }
    if event_type == "add_to_cart":
        base["product_id"]   = f"prod_{random.randint(1000,9999)}"
        base["product_name"] = random.choice(["Widget Pro","Gadget X","Thing Plus","Doohickey"])
        base["price_usd"]    = round(random.uniform(9.99, 299.99), 2)
        base["quantity"]     = random.randint(1, 5)
    elif event_type == "checkout_start":
        base["cart_total"]   = round(random.uniform(20, 800), 2)
        base["item_count"]   = random.randint(1, 8)
    elif event_type == "search":
        base["query"]         = random.choice(["widget","best price","review","compare","discount"])
        base["results_count"] = random.randint(0, 50)
    elif event_type == "product_view":
        base["product_id"]   = f"prod_{random.randint(1000,9999)}"
        base["time_on_page"] = random.randint(5, 300)
    return base

def build_context(d: date, schema_drift_day: date) -> dict:
    if d >= schema_drift_day:
        return {
            "library":   {"name": "analytics.js", "version": "2.11.1"},
            "userAgent": "Mozilla/5.0 (compatible)",
            "ip":        f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
            "locale":    random.choice(["en-US","en-GB","fr-FR","de-DE"]),
        }
    return {
        "library_name":    "analytics.js",
        "library_version": "2.9.0",
        "user_agent":      "Mozilla/5.0",
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

# Detect existing state
first_date   = dataset_first_date()
g_last       = google_last_date()
m_last       = meta_last_date()
l_last       = linkedin_last_date()
s_last       = segment_last_date()
p_last       = postgres_last_date()
next_conv_id = postgres_last_conversion_id() + 1
is_fresh     = g_last is None

# Each source starts from the day after its last generated date
google_start   = (g_last + timedelta(days=1)) if g_last   else FIRST_DATE
meta_start     = (m_last + timedelta(days=1)) if m_last   else FIRST_DATE
linkedin_start = (l_last + timedelta(days=1)) if l_last   else FIRST_DATE
segment_start  = (s_last + timedelta(days=1)) if s_last   else FIRST_DATE
postgres_start = (p_last + timedelta(days=1)) if p_last   else FIRST_DATE

if is_fresh:
    print(f"Fresh run — generating full year: {FIRST_DATE} → {TODAY}")
else:
    print(f"Incremental run — appending new days up to {TODAY}")
    print(f"  Google Ads last date   : {g_last}")
    print(f"  Meta Ads last date     : {m_last}")
    print(f"  LinkedIn Ads last date : {l_last}")
    print(f"  Segment last date      : {s_last}")
    print(f"  Postgres last date     : {p_last}")
    print(f"  Next conversion_id     : {next_conv_id}")

# Build campaign windows and cutoffs anchored to the dataset's first date
CAMPAIGN_WINDOWS       = build_campaign_windows(first_date, TODAY)
SCHEMA_DRIFT_DAY       = first_date + timedelta(days=180)
MISSING_SESSION_CUTOFF = first_date + timedelta(days=90)

google_campaigns   = [c for c in CAMPAIGNS if c[2] == "google"]
meta_campaigns     = [c for c in CAMPAIGNS if c[2] == "meta"]
linkedin_campaigns = [c for c in CAMPAIGNS if c[2] == "linkedin"]


# ── 1. Google Ads ─────────────────────────────────────────────────────────────
# Columns: date, campaign_id, campaign_name, channel, spend_usd,
#          impressions, clicks, campaign_status, soft_deleted

new_google_days = list(daterange(google_start, TODAY))

if not new_google_days:
    print("\nGoogle Ads   : already up to date.")
else:
    print(f"\nGenerating Google Ads CSVs for {len(new_google_days)} new day(s) …")
    for d in new_google_days:
        rows = []
        for cid, name, platform, channel, budget in google_campaigns:
            if not campaign_active_on(cid, d, CAMPAIGN_WINDOWS):
                continue
            mult   = surge_multiplier(d)
            spend  = jitter(budget * mult * random.uniform(0.5, 1.0))
            impr   = int(spend * random.uniform(80, 200))
            clicks = int(impr  * random.uniform(0.01, 0.08))
            row = {
                "date":            google_messy_date(d),
                "campaign_id":     str(cid),
                "campaign_name":   google_messy_campaign_name(name),
                "channel":         google_messy_channel(channel),
                "spend_usd":       spend,
                "impressions":     impr,
                "clicks":          clicks,
                "campaign_status": google_campaign_status(cid, d, CAMPAIGN_WINDOWS),
                "soft_deleted":    "false",
            }
            rows.append(row)
            # ~3% chance of a soft-deleted duplicate row
            if random.random() < 0.03:
                dup = dict(row)
                dup["soft_deleted"] = "true"
                rows.append(dup)

        if rows:
            fname = GOOGLE_DIR / f"google_ads_{d.strftime('%Y%m%d')}.csv"
            with open(fname, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

    total_google = len(list(GOOGLE_DIR.iterdir()))
    print(f"  → {len(new_google_days)} new CSV file(s)  ({total_google} total)")


# ── 2. Meta Ads ───────────────────────────────────────────────────────────────
# Columns differ from Google on purpose to exercise Bronze normalisation:
#   report_date      ← date
#   campaign_id
#   campaign_name
#   placement_type   ← channel  (Meta calls it "placement")
#   amount_spent     ← spend_usd
#   reach            ← impressions  (Meta reports reach, not raw impressions)
#   link_clicks      ← clicks
#   effective_status ← campaign_status as uppercase ("ACTIVE"/"PAUSED"/"DELETED")
#   currency         ← always "USD" but occasionally omitted entirely
#   is_deleted       ← "True" / "False"  (Python-style, not "true"/"false")

new_meta_days = list(daterange(meta_start, TODAY))

if not new_meta_days:
    print("\nMeta Ads     : already up to date.")
else:
    print(f"\nGenerating Meta Ads CSVs for {len(new_meta_days)} new day(s) …")
    for d in new_meta_days:
        rows = []
        for cid, name, platform, channel, budget in meta_campaigns:
            if not campaign_active_on(cid, d, CAMPAIGN_WINDOWS):
                continue
            mult   = surge_multiplier(d)
            spend  = jitter(budget * mult * random.uniform(0.5, 1.0))
            # Meta reports "reach" (unique users) rather than raw impression count
            reach  = int(spend * random.uniform(40, 120))
            clicks = int(reach  * random.uniform(0.01, 0.06))
            row = {
                "report_date":    meta_messy_date(d),
                "campaign_id":    str(cid),
                "campaign_name":  meta_messy_campaign_name(name),
                "placement_type": meta_messy_channel(channel),
                "amount_spent":   spend,
                "reach":          reach,
                "link_clicks":    clicks,
                "effective_status": meta_campaign_status(cid, d, CAMPAIGN_WINDOWS),
                "is_deleted":     "False",
            }
            # ~80% of rows include the currency field; rest omit it entirely
            if random.random() < 0.80:
                row["currency"] = "USD"
            rows.append(row)
            # ~3% chance of a soft-deleted duplicate
            if random.random() < 0.03:
                dup = dict(row)
                dup["is_deleted"]      = "True"
                dup["effective_status"] = "DELETED"
                rows.append(dup)

        if rows:
            fname = META_DIR / f"meta_ads_{d.strftime('%Y%m%d')}.csv"
            # fieldnames must be stable across rows even when currency is absent
            all_fields = ["report_date", "campaign_id", "campaign_name",
                          "placement_type", "amount_spent", "reach",
                          "link_clicks", "effective_status", "currency", "is_deleted"]
            with open(fname, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

    total_meta = len(list(META_DIR.iterdir()))
    print(f"  → {len(new_meta_days)} new CSV file(s)  ({total_meta} total)")


# ── 3. LinkedIn Ads ───────────────────────────────────────────────────────────
# Columns differ from both Google and Meta:
#   start_date       ← date  (LinkedIn calls it "start_date" in exports)
#   campaign_id
#   campaign_name
#   ad_type          ← channel  (LinkedIn calls it "ad type")
#   cost_in_usd      ← spend_usd
#   impressions      ← same name but different scale
#   total_clicks     ← clicks
#   campaign_status  ← mixed-case status string; no explicit deleted flag

new_linkedin_days = list(daterange(linkedin_start, TODAY))

if not new_linkedin_days:
    print("\nLinkedIn Ads : already up to date.")
else:
    print(f"\nGenerating LinkedIn Ads CSVs for {len(new_linkedin_days)} new day(s) …")
    for d in new_linkedin_days:
        rows = []
        for cid, name, platform, channel, budget in linkedin_campaigns:
            if not campaign_active_on(cid, d, CAMPAIGN_WINDOWS):
                continue
            mult   = surge_multiplier(d)
            spend  = jitter(budget * mult * random.uniform(0.5, 1.0))
            # LinkedIn B2B CPMs are higher so impression volumes are lower
            impr   = int(spend * random.uniform(20, 80))
            clicks = int(impr  * random.uniform(0.005, 0.04))
            row = {
                "start_date":      linkedin_messy_date(d),
                "campaign_id":     str(cid),
                "campaign_name":   linkedin_messy_campaign_name(name),
                "ad_type":         linkedin_messy_channel(channel),
                "cost_in_usd":     spend,
                "impressions":     impr,
                "total_clicks":    clicks,
                "campaign_status": random.choice(LINKEDIN_CAMPAIGN_STATUSES),
            }
            rows.append(row)
            # LinkedIn exports occasionally include zero-spend rows for paused campaigns
            if random.random() < 0.04:
                paused_row = dict(row)
                paused_row["cost_in_usd"]     = 0.0
                paused_row["impressions"]      = 0
                paused_row["total_clicks"]     = 0
                paused_row["campaign_status"]  = "PAUSED"
                rows.append(paused_row)

        if rows:
            fname = LINKEDIN_DIR / f"linkedin_ads_{d.strftime('%Y%m%d')}.csv"
            with open(fname, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

    total_linkedin = len(list(LINKEDIN_DIR.iterdir()))
    print(f"  → {len(new_linkedin_days)} new CSV file(s)  ({total_linkedin} total)")


# ── 4. Segment ────────────────────────────────────────────────────────────────

new_segment_days = list(daterange(segment_start, TODAY))

if not new_segment_days:
    print("\nSegment      : already up to date.")
else:
    print(f"\nGenerating Segment events for {len(new_segment_days)} new day(s) …")
    seg_file = SEGMENT_DIR / "segment_tracks.jsonl"

    existing_count = 0
    if seg_file.exists():
        with open(seg_file) as f:
            for _ in f:
                existing_count += 1

    new_events = 0
    with open(seg_file, "a") as f:
        for d in new_segment_days:
            active   = [c for c in CAMPAIGNS if campaign_active_on(c[0], d, CAMPAIGN_WINDOWS)]
            n_events = int(len(active) * random.uniform(30, 120) * surge_multiplier(d))

            for _ in range(n_events):
                cid, name, platform, channel, _ = random.choice(active) if active else CAMPAIGNS[0]
                uid        = random.choice(USER_IDS)
                event_type = random.choice(EVENT_TYPES)
                global_idx = existing_count + new_events

                event = {
                    "message_id":   hashlib.md5(f"{d}{uid}{global_idx}".encode()).hexdigest(),
                    "type":         "track",
                    "event":        event_type,
                    "timestamp":    segment_timestamp(d, SCHEMA_DRIFT_DAY),
                    "user_id":      uid,
                    "anonymous_id": f"anon_{random.randint(100000,999999)}",
                    "properties":   build_properties(event_type, cid, channel),
                    "context":      build_context(d, SCHEMA_DRIFT_DAY),
                }

                if d >= MISSING_SESSION_CUTOFF:
                    event["session_id"] = f"sess_{random.randint(10000000,99999999)}"

                if random.random() < 0.04:
                    event["user_id"] = random.choice(NULL_VARIANTS)

                f.write(json.dumps(event) + "\n")
                new_events += 1

    total_events = existing_count + new_events
    print(f"  → {new_events:,} new events appended  ({total_events:,} total)")


# ── 5. Postgres ───────────────────────────────────────────────────────────────

new_postgres_days = list(daterange(postgres_start, TODAY))

if not new_postgres_days:
    print("\nPostgres     : already up to date.")
else:
    print(f"\nGenerating Postgres conversions for {len(new_postgres_days)} new day(s) …")
    sql_file      = POSTGRES_DIR / "conversions.sql"
    conversion_id = next_conv_id
    new_rows      = 0
    write_header  = not sql_file.exists()

    with open(sql_file, "a") as f:
        if write_header:
            f.write("-- Conversions export from app Postgres backend\n")
            f.write(f"-- First generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
            f.write(
                "CREATE TABLE IF NOT EXISTS app_conversions (\n"
                "    conversion_id   INTEGER PRIMARY KEY,\n"
                "    usr_id          INTEGER,\n"
                "    cmpgn_id        INTEGER,\n"
                "    conv_type_cd    INTEGER,\n"
                "    revenue_amt     NUMERIC(10,2),\n"
                "    conv_ts         VARCHAR(32),\n"
                "    created_at      VARCHAR(32)\n"
                ");\n\n"
            )
        else:
            f.write(f"\n-- Appended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")

        for d in new_postgres_days:
            active = [c for c in CAMPAIGNS if campaign_active_on(c[0], d, CAMPAIGN_WINDOWS)]
            if not active:
                continue

            n_conversions = int(len(active) * random.uniform(0.5, 4) * surge_multiplier(d))

            for _ in range(n_conversions):
                cid, _, __, ___, ____ = random.choice(active)
                uid_int   = random.choice(USER_ID_INTS)
                conv_type = random.choices([1, 2, 3, 4], weights=[40, 30, 20, 10])[0]

                if conv_type == 1:
                    revenue = round(random.uniform(9.99, 499.99), 2)
                elif conv_type == 4:
                    revenue = round(random.uniform(500, 5000), 2)
                else:
                    revenue = None

                dt = datetime(d.year, d.month, d.day,
                              random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
                conv_ts    = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
                created_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                rev_sql    = "NULL" if revenue is None else str(revenue)

                f.write(
                    f"INSERT INTO app_conversions VALUES "
                    f"({conversion_id}, {uid_int}, {cid}, {conv_type}, "
                    f"{rev_sql}, '{conv_ts}', '{created_at}');\n"
                )
                conversion_id += 1
                new_rows      += 1

    print(f"  → {new_rows:,} new rows appended  ({conversion_id - 1:,} total)")


# ── Summary ───────────────────────────────────────────────────────────────────

print("\n─────────────────────────────────────────────────────────")
print(f"  {'Fresh generation' if is_fresh else 'Incremental update'} complete!")
print(f"  Dataset range  : {first_date} → {TODAY}")
print(f"  Google Ads     : {len(list(GOOGLE_DIR.iterdir()))} CSV files total")
print(f"  Meta Ads       : {len(list(META_DIR.iterdir()))} CSV files total")
print(f"  LinkedIn Ads   : {len(list(LINKEDIN_DIR.iterdir()))} CSV files total")
print("─────────────────────────────────────────────────────────")
