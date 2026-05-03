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
                  trailing/leading whitespace in campaign names, mixed-case channel
                  variants, campaign_status as lowercase strings, creative format
                  stored as "ad_format" with Google-specific values
  - Meta Ads:     different column names (report_date, amount_spent, reach,
                  link_clicks, placement_type), Meta-specific channel taxonomy,
                  is_deleted as Python bool strings ("True"/"False"), occasional
                  missing currency field, effective_status as uppercase strings,
                  creative format stored as "creative_type" with Meta-specific values
  - LinkedIn Ads: different column names (start_date, cost_in_usd, total_clicks,
                  ad_type), LinkedIn-specific channel taxonomy, campaign_status with
                  mixed-case values, creative format stored as "format" with
                  LinkedIn-specific values
  - Segment:      nested JSON payloads, missing fields on older events, inconsistent
                  null representations, schema drift mid-year, product references
                  drawn from a fixed product catalogue
  - Postgres:     snake_case naming from backend, integer IDs, UTC offset timezone
                  strings, product FK (prd_id) on purchase conversions (conv_type_cd=1)

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
FIRST_DATE = TODAY - timedelta(days=365)

# ── Output dirs ──────────────────────────────────────────────────────────────

RAW_DIR      = Path("raw_data")
GOOGLE_DIR   = RAW_DIR / "google_ads"
META_DIR     = RAW_DIR / "meta_ads"
LINKEDIN_DIR = RAW_DIR / "linkedin_ads"
SEGMENT_DIR  = RAW_DIR / "segment"
POSTGRES_DIR = RAW_DIR / "postgres"

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

# ── Products catalogue ────────────────────────────────────────────────────────
# Fixed set of 20 products shared across Segment events and Postgres conversions.
# Segment references products by string ID ("prod_1001") and name.
# Postgres references them by integer ID (1001) in purchase conversions.
# Bridge: strip "prod_" prefix — "prod_1001" <-> 1001.

PRODUCTS = [
    # (id, name, category, price_usd)
    (1001, "Widget Pro",          "hardware",  49.99),
    (1002, "Gadget X",            "hardware",  89.99),
    (1003, "Thing Plus",          "hardware",  29.99),
    (1004, "Doohickey",           "hardware",  19.99),
    (1005, "SuperWidget",         "hardware", 129.99),
    (1006, "Basic Plan",          "software",   9.99),
    (1007, "Pro Plan",            "software",  29.99),
    (1008, "Enterprise Plan",     "software",  99.99),
    (1009, "Add-on Pack A",       "software",  14.99),
    (1010, "Add-on Pack B",       "software",  24.99),
    (1011, "Starter Kit",         "bundle",    59.99),
    (1012, "Professional Bundle", "bundle",   149.99),
    (1013, "Ultimate Bundle",     "bundle",   249.99),
    (1014, "Holiday Special",     "bundle",    79.99),
    (1015, "Back to School Kit",  "bundle",    69.99),
    (1016, "Extended Warranty",   "service",   19.99),
    (1017, "Priority Support",    "service",   49.99),
    (1018, "Setup Service",       "service",   39.99),
    (1019, "Training Course",     "service",   79.99),
    (1020, "Consulting Hour",     "service",  199.99),
]

PRODUCTS_LIST = PRODUCTS   # alias used in generation loops

# ── Creatives catalogue ───────────────────────────────────────────────────────
# Each campaign has 2-3 creatives. Creative IDs are globally unique integers.
# Canonical formats: "image", "video", "carousel", "text".
# Each platform stores format in its own field name with its own variant strings.
#
# Structure: {campaign_id: [(creative_id, creative_name, canonical_format), ...]}

CREATIVES = {
    1:  [(101, "Brand Hero Banner",       "image"),
         (102, "Brand Story Video",       "video")],
    2:  [(103, "Summer Sale Static",      "image"),
         (104, "Summer Sale Carousel",    "carousel"),
         (105, "Summer Retargeting GIF",  "image")],
    3:  [(106, "Launch Teaser 15s",       "video"),
         (107, "Launch Demo 30s",         "video")],
    4:  [(108, "Holiday Gift Guide",      "carousel"),
         (109, "Holiday Promo Banner",    "image")],
    5:  [(110, "Acquisition Text Ad",     "text"),
         (111, "Acquisition Display",     "image")],
    6:  [(112, "Brand Awareness Social",  "image"),
         (113, "Brand Story Reel",        "video")],
    7:  [(114, "Summer Carousel Meta",    "carousel"),
         (115, "Summer Story Ad",         "image")],
    8:  [(116, "Cart Abandon Reminder",   "image"),
         (117, "Cart Abandon Carousel",   "carousel")],
    9:  [(118, "Holiday Catalog Ad",      "carousel"),
         (119, "Holiday Video Meta",      "video")],
    10: [(120, "Lookalike Static",        "image"),
         (121, "Lookalike Video",         "video")],
    11: [(122, "Brand Text Ad LinkedIn",  "text"),
         (123, "Brand Spotlight",         "image")],
    12: [(124, "Lead Gen Form Ad",        "image"),
         (125, "Lead Gen Video",          "video"),
         (126, "Lead Gen Carousel",       "carousel")],
    13: [(127, "Thought Leadership Doc",  "image"),
         (128, "TL Video LinkedIn",       "video")],
    14: [(129, "Event Banner",            "image"),
         (130, "Event Video Teaser",      "video")],
    15: [(131, "Demo Offer Text",         "text"),
         (132, "Demo Offer Image",        "image")],
    16: [(133, "Competitor Text Ad",      "text"),
         (134, "Competitor Display",      "image")],
    17: [(135, "App Install Banner",      "image"),
         (136, "App Install Video",       "video")],
    18: [(137, "Podcast Audio Ad",        "video"),
         (138, "Podcast Companion Image", "image")],
    19: [(139, "Influencer Reel",         "video"),
         (140, "Influencer Story",        "image")],
    20: [(141, "Career Page Banner",      "image"),
         (142, "Career Story Video",      "video")],
    21: [(143, "DSA Text Ad A",           "text"),
         (144, "DSA Text Ad B",           "text")],
    22: [(145, "YouTube Bumper 6s",       "video"),
         (146, "YouTube Bumper Alt",      "video")],
    23: [(147, "Spring Collection Reel",  "video"),
         (148, "Spring Catalog Carousel", "carousel")],
    24: [(149, "Back to School Banner",   "image"),
         (150, "BTS Shopping Carousel",   "carousel")],
    25: [(151, "Flash Sale Text",         "text"),
         (152, "Flash Sale Image",        "image")],
}

# 5000 users shared across Segment and Postgres.
USER_IDS     = [f"u_{i:05d}" for i in range(1, 5001)]
USER_ID_INTS = list(range(1, 5001))

EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "search", "product_view"]


# ── Incremental state detection ───────────────────────────────────────────────

def _last_date_from_csv_dir(directory: Path, prefix: str) -> date | None:
    files = sorted(directory.glob(f"{prefix}_*.csv"))
    if not files:
        return None
    stem = files[-1].stem.replace(f"{prefix}_", "")
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
# trailing/leading whitespace in campaign names, soft-deleted duplicates,
# campaign_status as lowercase, creative format as "ad_format" with
# Google-specific values (IMAGE_AD, VIDEO_AD, MULTI_IMAGE_AD, RESPONSIVE_SEARCH_AD)

GOOGLE_DATE_FMTS = ["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"]

GOOGLE_CHANNEL_VARIANTS = {
    "search":   ["search", "Paid Search", "paid_search", "SEARCH", "Search"],
    "display":  ["display", "Display", "DISPLAY", "display_network"],
    "video":    ["video", "Video", "VIDEO", "youtube_video"],
    "shopping": ["shopping", "Shopping", "SHOPPING", "product_shopping"],
}

GOOGLE_FORMAT_VARIANTS = {
    "image":    ["IMAGE_AD", "image_ad", "Image Ad", "DISPLAY_IMAGE"],
    "video":    ["VIDEO_AD", "video_ad", "Video Ad", "IN_STREAM_VIDEO"],
    "carousel": ["MULTI_IMAGE_AD", "multi_image_ad", "Multi Image", "RESPONSIVE_DISPLAY_AD"],
    "text":     ["RESPONSIVE_SEARCH_AD", "responsive_search_ad", "Text Ad", "EXPANDED_TEXT_AD"],
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

def google_messy_format(canonical_format: str) -> str:
    return random.choice(GOOGLE_FORMAT_VARIANTS[canonical_format])

def google_campaign_status(cid: int, d: date, windows: dict) -> str:
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    if d > end:
        return random.choice(["paused", "removed"])
    return random.choices(["enabled", "paused"], weights=[92, 8])[0]


# ── Meta Ads helpers ──────────────────────────────────────────────────────────
# Messiness: different column names, Meta-specific channel taxonomy,
# is_deleted as Python bool strings, occasional missing currency,
# effective_status as uppercase, creative format as "creative_type" with
# Meta-specific values (single_image, single_video, carousel, text_only)

META_DATE_FMTS = ["%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"]

META_CHANNEL_VARIANTS = {
    "search":   ["Paid Social", "paid_social", "PAID_SOCIAL", "Advantage+ Audience", "advantage_audience"],
    "display":  ["Feed", "feed", "Instagram Feed", "facebook_feed", "FEED", "Stories"],
    "video":    ["Reels", "reels", "In-Stream Video", "REELS", "in_stream_video", "Video Feed"],
    "shopping": ["Catalog Sales", "catalog_sales", "CATALOG_SALES", "dynamic_ads", "Shopping"],
}

META_FORMAT_VARIANTS = {
    "image":    ["single_image", "SINGLE_IMAGE", "Single Image", "static_image"],
    "video":    ["single_video", "SINGLE_VIDEO", "Video", "reel_video"],
    "carousel": ["carousel", "CAROUSEL", "Carousel Ad", "multi_asset"],
    "text":     ["text_only", "TEXT_ONLY", "Text Ad", "link_ad"],
}

def meta_messy_date(d: date) -> str:
    return d.strftime(random.choice(META_DATE_FMTS))

def meta_messy_channel(channel: str) -> str:
    return random.choice(META_CHANNEL_VARIANTS[channel])

def meta_messy_campaign_name(name: str) -> str:
    if random.random() < 0.10:
        name = name + " "
    if random.random() < 0.08:
        name = name + " | Meta"
    return name

def meta_messy_format(canonical_format: str) -> str:
    return random.choice(META_FORMAT_VARIANTS[canonical_format])

def meta_campaign_status(cid: int, d: date, windows: dict) -> str:
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    if d > end:
        return random.choice(["PAUSED", "DELETED"])
    return random.choices(["ACTIVE", "PAUSED"], weights=[92, 8])[0]


# ── LinkedIn Ads helpers ──────────────────────────────────────────────────────
# Messiness: different column names, LinkedIn-specific channel taxonomy,
# mixed-case campaign_status, creative format as "format" with
# LinkedIn-specific values (SINGLE_IMAGE, VIDEO, CAROUSEL, TEXT_AD)

LINKEDIN_DATE_FMTS = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"]

LINKEDIN_CHANNEL_VARIANTS = {
    "search":   ["Sponsored Content", "Text Ads", "text_ads", "SPONSORED_CONTENT", "sponsored_content"],
    "display":  ["Display Ads", "display_ads", "DISPLAY", "Programmatic Display", "programmatic_display"],
    "video":    ["Video Ads", "video_ads", "SPONSORED_VIDEO", "In-Stream Video", "in_stream_video"],
    "shopping": ["Dynamic Ads", "dynamic_ads", "DYNAMIC", "Spotlight Ads", "spotlight_ads"],
}

LINKEDIN_FORMAT_VARIANTS = {
    "image":    ["SINGLE_IMAGE", "Single Image", "single_image", "STANDARD_UPDATE"],
    "video":    ["VIDEO", "Video", "video", "SPONSORED_VIDEO"],
    "carousel": ["CAROUSEL", "Carousel", "carousel", "MULTI_IMAGE"],
    "text":     ["TEXT_AD", "Text Ad", "text_ad", "SPOTLIGHT"],
}

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

def linkedin_messy_format(canonical_format: str) -> str:
    return random.choice(LINKEDIN_FORMAT_VARIANTS[canonical_format])

def linkedin_campaign_status(cid: int, d: date, windows: dict) -> str:
    start, end = windows.get(cid, (FIRST_DATE, TODAY))
    if d > end:
        return random.choice(["PAUSED", "COMPLETED"])
    return random.choices(["ACTIVE", "active", "Active", "PAUSED"], weights=[70, 10, 10, 10])[0]


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
        pid, pname, pcat, pprice = random.choice(PRODUCTS_LIST)
        base["product_id"]   = f"prod_{pid}"
        base["product_name"] = pname
        base["category"]     = pcat
        base["price_usd"]    = pprice
        base["quantity"]     = random.randint(1, 5)
    elif event_type == "checkout_start":
        base["cart_total"]   = round(random.uniform(20, 800), 2)
        base["item_count"]   = random.randint(1, 8)
    elif event_type == "search":
        base["query"]         = random.choice(["widget","best price","review","compare","discount"])
        base["results_count"] = random.randint(0, 50)
    elif event_type == "product_view":
        pid, pname, pcat, pprice = random.choice(PRODUCTS_LIST)
        base["product_id"]   = f"prod_{pid}"
        base["product_name"] = pname
        base["category"]     = pcat
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

first_date   = dataset_first_date()
g_last       = google_last_date()
m_last       = meta_last_date()
l_last       = linkedin_last_date()
s_last       = segment_last_date()
p_last       = postgres_last_date()
next_conv_id = postgres_last_conversion_id() + 1
is_fresh     = g_last is None

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

CAMPAIGN_WINDOWS       = build_campaign_windows(first_date, TODAY)
SCHEMA_DRIFT_DAY       = first_date + timedelta(days=180)
MISSING_SESSION_CUTOFF = first_date + timedelta(days=90)

google_campaigns   = [c for c in CAMPAIGNS if c[2] == "google"]
meta_campaigns     = [c for c in CAMPAIGNS if c[2] == "meta"]
linkedin_campaigns = [c for c in CAMPAIGNS if c[2] == "linkedin"]

PRODUCTS_LIST = PRODUCTS


# ── 1. Google Ads ─────────────────────────────────────────────────────────────
# Grain: one row per campaign / creative / channel / day
# Columns: date, campaign_id, campaign_name, creative_id, creative_name,
#          ad_format, channel, spend_usd, impressions, clicks,
#          campaign_status, soft_deleted

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
            campaign_creatives = CREATIVES.get(cid, [])
            mult = surge_multiplier(d)
            for cr_id, cr_name, cr_fmt in campaign_creatives:
                cr_share = random.uniform(0.3, 0.7)
                spend  = jitter(budget * mult * random.uniform(0.5, 1.0) * cr_share)
                impr   = int(spend * random.uniform(80, 200))
                clicks = int(impr  * random.uniform(0.01, 0.08))
                row = {
                    "date":            google_messy_date(d),
                    "campaign_id":     str(cid),
                    "campaign_name":   google_messy_campaign_name(name),
                    "creative_id":     str(cr_id),
                    "creative_name":   cr_name,
                    "ad_format":       google_messy_format(cr_fmt),
                    "channel":         google_messy_channel(channel),
                    "spend_usd":       spend,
                    "impressions":     impr,
                    "clicks":          clicks,
                    "campaign_status": google_campaign_status(cid, d, CAMPAIGN_WINDOWS),
                    "soft_deleted":    "false",
                }
                rows.append(row)
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
# Grain: one row per campaign / creative / channel / day
# Columns: report_date, campaign_id, campaign_name, creative_id, creative_name,
#          creative_type, placement_type, amount_spent, reach, link_clicks,
#          effective_status, currency (sometimes absent), is_deleted

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
            campaign_creatives = CREATIVES.get(cid, [])
            mult = surge_multiplier(d)
            for cr_id, cr_name, cr_fmt in campaign_creatives:
                cr_share = random.uniform(0.3, 0.7)
                spend  = jitter(budget * mult * random.uniform(0.5, 1.0) * cr_share)
                reach  = int(spend * random.uniform(40, 120))
                clicks = int(reach  * random.uniform(0.01, 0.06))
                row = {
                    "report_date":      meta_messy_date(d),
                    "campaign_id":      str(cid),
                    "campaign_name":    meta_messy_campaign_name(name),
                    "creative_id":      str(cr_id),
                    "creative_name":    cr_name,
                    "creative_type":    meta_messy_format(cr_fmt),
                    "placement_type":   meta_messy_channel(channel),
                    "amount_spent":     spend,
                    "reach":            reach,
                    "link_clicks":      clicks,
                    "effective_status": meta_campaign_status(cid, d, CAMPAIGN_WINDOWS),
                    "is_deleted":       "False",
                }
                if random.random() < 0.80:
                    row["currency"] = "USD"
                rows.append(row)
                if random.random() < 0.03:
                    dup = dict(row)
                    dup["is_deleted"]       = "True"
                    dup["effective_status"] = "DELETED"
                    rows.append(dup)

        if rows:
            fname = META_DIR / f"meta_ads_{d.strftime('%Y%m%d')}.csv"
            all_fields = ["report_date", "campaign_id", "campaign_name",
                          "creative_id", "creative_name", "creative_type",
                          "placement_type", "amount_spent", "reach",
                          "link_clicks", "effective_status", "currency", "is_deleted"]
            with open(fname, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

    total_meta = len(list(META_DIR.iterdir()))
    print(f"  → {len(new_meta_days)} new CSV file(s)  ({total_meta} total)")


# ── 3. LinkedIn Ads ───────────────────────────────────────────────────────────
# Grain: one row per campaign / creative / channel / day
# Columns: start_date, campaign_id, campaign_name, creative_id, creative_name,
#          format, ad_type, cost_in_usd, impressions, total_clicks,
#          campaign_status
# Note: no explicit deleted flag — paused zero-spend rows signal inactive creatives

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
            campaign_creatives = CREATIVES.get(cid, [])
            mult = surge_multiplier(d)
            for cr_id, cr_name, cr_fmt in campaign_creatives:
                cr_share = random.uniform(0.3, 0.7)
                spend  = jitter(budget * mult * random.uniform(0.5, 1.0) * cr_share)
                impr   = int(spend * random.uniform(20, 80))
                clicks = int(impr  * random.uniform(0.005, 0.04))
                row = {
                    "start_date":      linkedin_messy_date(d),
                    "campaign_id":     str(cid),
                    "campaign_name":   linkedin_messy_campaign_name(name),
                    "creative_id":     str(cr_id),
                    "creative_name":   cr_name,
                    "format":          linkedin_messy_format(cr_fmt),
                    "ad_type":         linkedin_messy_channel(channel),
                    "cost_in_usd":     spend,
                    "impressions":     impr,
                    "total_clicks":    clicks,
                    "campaign_status": linkedin_campaign_status(cid, d, CAMPAIGN_WINDOWS),
                }
                rows.append(row)
                if random.random() < 0.04:
                    paused_row = dict(row)
                    paused_row["cost_in_usd"]    = 0.0
                    paused_row["impressions"]     = 0
                    paused_row["total_clicks"]    = 0
                    paused_row["campaign_status"] = "PAUSED"
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
# add_to_cart and product_view events reference products from the fixed
# PRODUCTS catalogue by string ID ("prod_1001") and include product_name
# and category. This gives dim_products real attributes to draw from.

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
# Purchase conversions (conv_type_cd = 1) include prd_id referencing a product
# from the PRODUCTS catalogue. Revenue is derived from product price × quantity
# rather than a random range, keeping it consistent with the catalogue.
# All other conversion types have NULL prd_id.

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
                "    prd_id          INTEGER,\n"
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
                    # Purchase: pick a real product; revenue = price × qty ± small jitter
                    pid, pname, pcat, pprice = random.choice(PRODUCTS_LIST)
                    qty     = random.randint(1, 3)
                    revenue = round(pprice * qty * random.uniform(0.9, 1.1), 2)
                    prd_sql = str(pid)
                elif conv_type == 4:
                    # High-value event (demo request): no product
                    revenue = round(random.uniform(500, 5000), 2)
                    prd_sql = "NULL"
                else:
                    revenue = None
                    prd_sql = "NULL"

                dt = datetime(d.year, d.month, d.day,
                              random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
                conv_ts    = dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
                created_at = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                rev_sql    = "NULL" if revenue is None else str(revenue)

                f.write(
                    f"INSERT INTO app_conversions VALUES "
                    f"({conversion_id}, {uid_int}, {cid}, {prd_sql}, {conv_type}, "
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
