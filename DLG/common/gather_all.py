from .brands_fetcher import BrandFetcher
from .websites_fetcher import PublisherFetcher
from .fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
from typing import Any, Dict, List

# basic logging for Cloud Functions / local runs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -----------------------
# Utility helpers
# -----------------------
def return_lookup(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build dict mapping id (string) -> full brand/publisher info dict (keys normalized to str)."""
    lookup = {}
    for dat in data or []:
        # some files may use int ids or string ids - normalize to string key
        key = str(dat.get("id") if "id" in dat else dat.get("pk", dat.get("value", "")))
        lookup[key] = dat
    return lookup


def get_brand_owner(brand_id, brands_lookup):
    """
    Given a brand ID, walk up to the top-level parent and return its name.
    brand_id may be numeric or string. brands_lookup keys are strings.
    """
    if brand_id is None:
        return None
    bid = str(brand_id)
    visited = set()
    # climb up until parent_id is falsy
    while bid and bid not in visited:
        visited.add(bid)
        brand_info = brands_lookup.get(bid)
        if not brand_info:
            # unknown id -> return None
            return None
        parent_id = brand_info.get("parent_id")
        # if parent_id missing or falsy, this is top-level owner
        if not parent_id:
            return brand_info.get("name")
        # step up
        bid = str(parent_id)
    return None


def decide_content_type(website_name: str, raw_content_type: str = None) -> str:
    """Map API content types / website strings to UI categories (Search, Social, Standard)."""
    if raw_content_type:
        raw = str(raw_content_type).lower()
        if raw in ("search", "google", "bing"):
            return "Search"
        if any(s in raw for s in ("social", "facebook", "youtube", "instagram", "tiktok")):
            return "Social"
        # treat video/text/all as Standard fallback
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
    """
    Merge stats + brand + website lookups, returning list of row dicts.
    Filters out aggregate "segment summary" rows and rows where website == 0/-1/"all".
    Keeps product-level rows (product numeric or labeled), otherwise skips aggregated 'all' product rows.
    """
    brands_lookup_raw = return_lookup(brands_data)
    websites_lookup_raw = return_lookup(websites_data)

    # also build simple id->name maps normalized to strings for fast mapping
    brands_name_map = {k: (v.get("name") or v.get("label") or k) for k, v in brands_lookup_raw.items()}
    websites_name_map = {k: (v.get("name") or v.get("label") or k) for k, v in websites_lookup_raw.items()}

    def map_brand_name(brand_id):
        return brands_name_map.get(str(brand_id), brand_id)

    def map_product_name(product_segment):
        """Product segment can be dict with id/label or a number/string id."""
        if product_segment is None:
            return None
        if isinstance(product_segment, dict):
            # prefer label returned by API (friendly)
            return product_segment.get("label") or map_brand_name(product_segment.get("id"))
        # otherwise treat as id -> try lookup
        return brands_name_map.get(str(product_segment), product_segment)

    def map_website_name(website_id):
        return websites_name_map.get(str(website_id), website_id)

    rows = []
    for entry in stats_data or []:
        segment = entry.get("segment", {}) or {}
        stats_list = entry.get("stats", []) or []

        # normalize incoming segment ids -> string for consistent handling
        seg_brand = segment.get("brand")
        brand_name = map_brand_name(seg_brand)
        brand_owner_name = get_brand_owner(seg_brand, brands_lookup_raw) or brand_name

        # product handling - prefer product label when provided by API
        product_segment = segment.get("product")  # could be dict or id
        product_name = map_product_name(product_segment)

        # website mapping
        website_segment = segment.get("website")
        website_name = map_website_name(website_segment)

        # raw content type if any
        raw_content_type = segment.get("content_type")

        # platform / page_type
        platform = segment.get("platform")
        page_type = segment.get("page_type")

        # Skip high-level aggregate rows:
        # - row where website indicates aggregate (0, -1, "all") OR website_name suggests "Segment summary"
        # - or product is missing/equals "all"/0 (we want product-level rows)
        skip = False
        if website_segment in (0, "0", -1, "-1", "all", None):
            # keep "Other" or literal 'Other' rows? the UI shows 'Other' — keep name 'Other' if lookup gives that.
            # if website is actual 0 and websites lookup maps to a friendly name, keep; otherwise skip.
            if str(website_name).lower() in ("0", "0.0", "-1", "-1.0", "all", ""):
                skip = True

        # Product-level filter: if product is missing or equals "all" or 0 -> skip aggregated product row
        if product_segment in (0, "0", "all", None) and not (isinstance(product_segment, dict) and product_segment.get("label")):
            # if product missing and the segment represents a parent-level aggregate, skip — we only want product rows
            skip = True

        # Also skip rows where segment itself looks like a "Segment summary"
        if str(website_name).lower().strip() == "segment summary":
            skip = True

        if skip:
            continue

        # For each stats entry (one or multiple), build rows
        for stat in stats_list:
            # optional: filter to only the requested period by checking stat.get("period") if needed
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
            # values & uncertainties
            for k, v in stat.get("values", {}).items():
                row[k] = v
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v
            rows.append(row)

    return rows


# -----------------------
# Cleaning for BigQuery
# -----------------------
def get_previous_month_first_day():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    previous_month_first_day = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)
    return previous_month_first_day.strftime("%Y-%m-01")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean merged DataFrame to match BQ schema:
    - rename fields
    - ensure columns exist
    - fill date
    - force content type mapping from website or raw_content_type
    - drop remaining summary rows if any
    """
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

    # Ensure all expected columns exist
    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # Remove any residual 'Segment summary' or other aggregates
    df = df[~df["MediaChannel"].astype(str).str.contains("segment summary", case=False, na=False)]
    df = df[~df["Product"].astype(str).str.contains("segment summary", case=False, na=False)]

    # Set Date to previous month first day
    df["Date"] = get_previous_month_first_day()

    # Force ContentType using MediaChannel OR raw_content_type
    df["ContentType"] = df.apply(lambda r: decide_content_type(r["MediaChannel"], r.get("raw_content_type")), axis=1)

    # Convert AdContacts to int with safe fallbacks
    if "AdContacts" in df.columns:
        df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)

    # Reorder columns to match BigQuery
    df = df.reindex(columns=expected_columns)
    df = df.drop_duplicates()
    # final sort making output deterministic
    df = df.sort_values(by=["BrandOwner", "Brand", "Product", "MediaChannel"]).reset_index(drop=True)
    return df


def get_correct_period():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    period = f"month_{previous_month_last_day.strftime('%Y%m01')}"
    return period


def run_adreal_pipeline(username, password, market="ro", parent_brand_ids=None):
    """
    Public function to fetch, merge and clean AdReal data.
    - parent_brand_ids can be parent brand IDs; function will fetch stats and merge
      but will drop aggregated product rows so only real product-level rows are returned.
    """
    if parent_brand_ids is None:
        parent_brand_ids = []

    period = get_correct_period()

    # Fetch brands & websites
    brand_fetcher = BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=period)

    publisher_fetcher = PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=period)

    # Fetch stats (we keep segments to include product & website)
    adreal_fetcher = AdRealFetcher(username=username, password=password, market=market)
    adreal_fetcher.login()
    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc,mobile,tablet",
        page_types="search,social,standard,display",
        segments="brand,product,content_type,website,page_type,platform",
        limit=1000000,
    )

    merged_rows = merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows)
    if df.empty:
        logger.warning("No rows returned from merge_data() - check inputs and lookups")
        return pd.DataFrame(columns=["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"])
    df = clean_data(df)
    return df
