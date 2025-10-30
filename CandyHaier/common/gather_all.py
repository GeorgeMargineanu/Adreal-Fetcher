from .brands_fetcher import BrandFetcher
from .websites_fetcher import PublisherFetcher
from .fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime, timedelta

def return_lookup(data):
    """Helper: build id -> full brand/publisher info dict."""
    return {dat["id"]: dat for dat in data}

def get_brand_owner(brand_id, brands_lookup):
    """
    Given a brand ID, find the top-level Brand owner by recursively 
    traversing the parent hierarchy.
    """
    current_id = brand_id
    owner_name = None
    
    # Traverse up the brand hierarchy until the top is reached (no parent_id)
    while current_id:
        brand_info = brands_lookup.get(current_id)
        if not brand_info:
            # If the ID is invalid, stop and return the last valid name found (or None)
            return owner_name 
        
        # Store the current name as the potential owner. This name will be the top-most 
        # when the loop terminates.
        owner_name = brand_info.get("name")
        
        parent_id = brand_info.get("parent_id")
        
        if not parent_id:
            # We hit the top level (no parent_id defined), so owner_name is the true owner.
            return owner_name
        
        # Move up to the parent level for the next iteration
        current_id = parent_id

    return owner_name

def get_owner_from_id(node_id, brands_lookup):
    """
    Climb parents from a given node_id until a root is found; return that root's name.
    """
    current_id = node_id
    owner_name = None
    while current_id:
        info = brands_lookup.get(current_id)
        if not info:
            return owner_name
        owner_name = info.get("name")
        parent_id = info.get("parent_id")
        if not parent_id:
            return owner_name
        current_id = parent_id
    return owner_name


def is_top_level_other(node_id, brands_lookup):
    info = brands_lookup.get(node_id)
    if not info:
        return False
    return (info.get("parent_id") is None) and (str(info.get("name", "")).strip().lower() == "other")


def normalize_owner(brand_id, product_raw, brands_lookup):
    """
    1) Try brand lineage (as today).
    2) If owner is 'Other' (top-level) or missing, try product lineage.
    3) Return the best owner string.
    """
    owner = get_owner_from_id(brand_id, brands_lookup) if brand_id else None

    # If owner is 'Other' at top-level (API bucket) or missing, try product lineage
    if (owner is None) or (owner.strip().lower() == "other" and is_top_level_other(brand_id, brands_lookup)):
        # Resolve product_id from raw (can be dict or id)
        product_id = None
        if product_raw is not None:
            if isinstance(product_raw, dict):
                product_id = product_raw.get("id") or product_raw.get("value")  # API sometimes uses 'id' or 'value'
            else:
                product_id = product_raw
        if product_id:
            product_owner = get_owner_from_id(product_id, brands_lookup)
            if product_owner:
                return product_owner

    return owner

def merge_data(stats_data, brands_data, websites_data):
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)

    all_rows = []
    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_id = segment.get("brand")
        brand_info = brands_lookup.get(brand_id, {})
        brand_name = brand_info.get("name", brand_id)

        # Robust Product resolution (keep what you had + also keep raw for owner fallback)
        product_raw = segment.get("product")
        product_name = None
        if product_raw is not None:
            if isinstance(product_raw, dict):
                product_name = product_raw.get("label", product_raw.get("name"))
            else:
                product_name = brands_lookup.get(product_raw, {}).get("name", product_raw)

        # ✅ NEW: owner that prefers product lineage when brand is a top-level 'Other'
        brand_owner_name = normalize_owner(brand_id, product_raw, brands_lookup)

        website_name = websites_lookup.get(segment.get("website"), {}).get("name", segment.get("website"))

        content_type = segment.get("content_type")
        if not content_type or content_type == "None":
            content_type = decide_content_type(website_name)

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "Product": product_name,
                "website_name": website_name,
                "platform": segment.get("platform", None),
                "content_type": content_type,
            }
            # values
            for k, v in stat.get("values", {}).items():
                row[k] = v
            # uncertainty
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v
            all_rows.append(row)

    return all_rows


def decide_content_type(website):
    """Fallback if API doesn't provide content_type."""
    if not isinstance(website, str) or not website:
        return "Unknown"
    lowered_website = website.lower()
    if "google." in lowered_website or "bing." in lowered_website:
        return "Search"
    if any(social in lowered_website for social in ["facebook", "instagram", "tiktok", "youtube"]):
        return "Social"
    return "Standard"

def get_previous_month_first_day():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    previous_month_first_day = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)
    return previous_month_first_day.strftime('%Y-%m-01')

def clean_data(df):
    """Clean merged DataFrame to match BigQuery schema."""
    # 1. Rename columns to match BQ schema
    df = df.rename(columns={
        "brand_owner_name": "BrandOwner",
        "brand_name": "Brand",
        # "Product" is now named directly in merge_data
        "website_name": "MediaChannel",
        "ad_cont": "AdContacts",
        "content_type": "ContentType",
    })

    # 2. Define expected columns (Matching your working output structure)
    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaChannel", "AdContacts"]
    
    # 3. Ensure expected columns exist
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # 4. Remove summaries from MediaChannel
    df = df[df["MediaChannel"] != "Segment summary"]

    # 5. Set Date to previous month first day
    df['Date'] = get_previous_month_first_day()

    # 6. Force ContentType using MediaChannel
    df["ContentType"] = df["MediaChannel"].apply(decide_content_type)

    # 7. Reorder columns and enforce BQ schema by selecting only expected columns.
    df = df.reindex(columns=expected_columns) 
    
    return df

def get_previous_month_range():
    """Return previous month in AdReal API range format (YYYYMM01,YYYYMMDD,month)."""
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    first_of_previous_month = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)

    start_str = first_of_previous_month.strftime("%Y%m%d")
    end_str = previous_month_last_day.strftime("%Y%m%d")
    return f"{start_str},{end_str},month"

def get_correct_period():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    period = f"month_{previous_month_last_day.strftime('%Y%m01')}"
    return period

def run_adreal_pipeline(username, password, market="ro", parent_brand_ids=None):
    """Fetch, merge, clean AdReal data and return a DataFrame."""
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

    period_range = get_previous_month_range()

    # Fetch stats
    adreal_fetcher = AdRealFetcher(username=username, password=password, market=market, period_range=period_range)
    adreal_fetcher.login()
    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc",
        page_types="search,social,standard",
        segments="brand,product,content_type,website",
        limit=1000000
    )

    merged_rows = merge_data(stats_data, brands_data, websites_data)
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_data(df)
    return df