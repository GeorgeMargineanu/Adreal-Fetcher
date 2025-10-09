from .brands_fetcher import BrandFetcher
from .websites_fetcher import PublisherFetcher
from .fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------
# Utility helpers
# -----------------------
def return_lookup(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build dict mapping id (string) -> full brand/publisher info dict."""
    lookup = {}
    for dat in data or []:
        key = str(dat.get("id") if "id" in dat else dat.get("pk", dat.get("value", "")))
        lookup[key] = dat
    return lookup


def get_brand_owner(brand_id, brands_lookup):
    """Walk up to top-level parent and return brand owner name."""
    if brand_id is None:
        return None
    bid = str(brand_id)
    visited = set()
    while bid and bid not in visited:
        visited.add(bid)
        brand_info = brands_lookup.get(bid)
        if not brand_info:
            return None
        parent_id = brand_info.get("parent_id")
        if not parent_id:
            return brand_info.get("name")
        bid = str(parent_id)
    return None


def decide_content_type(website_name: str, raw_content_type: str = None) -> str:
    """Map content types / website strings to UI categories."""
    if raw_content_type:
        raw = str(raw_content_type).lower()
        if raw in ("search", "google", "bing"):
            return "Search"
        if any(s in raw for s in ("social", "facebook", "youtube", "instagram", "tiktok")):
            return "Social"

    if not isinstance(website_name, str) or not website_name:
        return "Standard"

    w = website_name.lower()
    if any(x in w for x in ("google.", "bing.", "yahoo.")):
        return "Search"
    if any(s in w for s in ("facebook", "youtube", "instagram", "tiktok", "x.com", "x.net")):
        return "Social"
    return "Standard"


# -----------------------
# Merge & cleaning
# -----------------------
def merge_data(stats_data, brands_data, websites_data):
    """Merge stats + brand + website lookups into row dicts."""
    brands_lookup_raw = return_lookup(brands_data)
    websites_lookup_raw = return_lookup(websites_data)

    brands_name_map = {k: (v.get("name") or v.get("label") or k) for k, v in brands_lookup_raw.items()}
    websites_name_map = {k: (v.get("name") or v.get("label") or k) for k, v in websites_lookup_raw.items()}

    def map_brand_name(brand_id):
        return brands_name_map.get(str(brand_id), brand_id)

    def map_product_name(product_segment):
        if product_segment is None:
            return None
        if isinstance(product_segment, dict):
            return product_segment.get("label") or map_brand_name(product_segment.get("id"))
        return brands_name_map.get(str(product_segment), product_segment)

    def map_website_name(website_id):
        return websites_name_map.get(str(website_id), website_id)

    rows = []
    for entry in stats_data or []:
        segment = entry.get("segment", {}) or {}
        stats_list = entry.get("stats", []) or []

        seg_brand = segment.get("brand")
        brand_name = map_brand_name(seg_brand)
        brand_owner_name = get_brand_owner(seg_brand, brands_lookup_raw) or brand_name

        product_segment = segment.get("product")
        product_name = map_product_name(product_segment)

        website_segment = segment.get("website")
        website_name = map_website_name(website_segment)

        raw_content_type = segment.get("content_type")
        platform = segment.get("platform")
        page_type = segment.get("page_type")

        # Skip aggregate rows
        skip = False
        if website_segment in (0, "0", -1, "-1", "all", None):
            if str(website_name).lower() in ("0", "-1", "all", ""):
                skip = True

        if product_segment in (0, "0", "all", None) and not (isinstance(product_segment, dict) and product_segment.get("label")):
            skip = True

        if str(website_name).lower().strip() == "segment summary":
            skip = True

        if skip:
            continue

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "product_label": product_name,
                "website_name": website_name,
                "platform": platform,
                "page_type": page_type,
                "raw_content_type": raw_content_type,
            }
            for k, v in stat.get("values", {}).items():
                row[k] = v
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v
            rows.append(row)

    return rows


# -----------------------
# BigQuery cleaning
# -----------------------
def get_previous_month_first_day():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    previous_month_first_day = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)
    return previous_month_first_day.strftime("%Y-%m-01")


def get_previous_month_range():
    """Return start and end dates of previous month in YYYYMMDD format."""
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    last_of_previous_month = first_of_current_month - timedelta(days=1)
    first_of_previous_month = datetime(last_of_previous_month.year, last_of_previous_month.month, 1)
    return first_of_previous_month.strftime("%Y%m%d"), last_of_previous_month.strftime("%Y%m%d")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean merged DataFrame to match BQ schema."""
    df = df.rename(
        columns={
            "brand_owner_name": "BrandOwner",
            "brand_name": "Brand",
            "product_label": "Product",
            "website_name": "MediaChannel",
            "ad_cont": "AdContacts",
            "raw_content_type": "raw_content_type",
        }
    )

    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    df = df[~df["MediaChannel"].astype(str).str.contains("segment summary", case=False, na=False)]
    df = df[~df["Product"].astype(str).str.contains("segment summary", case=False, na=False)]

    df["Date"] = get_previous_month_first_day()
    df["ContentType"] = df.apply(lambda r: decide_content_type(r["MediaChannel"], r.get("raw_content_type")), axis=1)

    if "AdContacts" in df.columns:
        df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)

    df = df.reindex(columns=expected_columns)
    df = df.drop_duplicates()
    df = df.sort_values(by=["BrandOwner", "Brand", "Product", "MediaChannel"]).reset_index(drop=True)
    return df


# -----------------------
# Main pipeline
# -----------------------
def run_adreal_pipeline(username, password, market="ro", parent_brand_ids=None):
    """
    Fetch, merge, and clean AdReal data for the previous month.
    Returns a DataFrame ready for BigQuery.
    """
    if parent_brand_ids is None:
        parent_brand_ids = []

    start_date, end_date = get_previous_month_range()
    logger.info(f"Fetching data for period: {start_date} - {end_date}")

    # Fetch brands & websites
    brand_fetcher = BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=f"month_{start_date}")

    publisher_fetcher = PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=f"month_{start_date}")

    # Fetch stats
    adreal_fetcher = AdRealFetcher(username=username, password=password, market=market)
    adreal_fetcher.login()
    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc,mobile,tablet",
        page_types="search,social,standard",
        segments="brand,product,content_type,website,page_type,platform",
        limit=1000000,
        periods_range=f"{start_date},{end_date},month"
    )

    merged_rows = merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows)

    if df.empty:
        logger.warning("No rows returned from merge_data() - check inputs and lookups")
        return pd.DataFrame(columns=["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"])

    df = clean_data(df)
    return df
