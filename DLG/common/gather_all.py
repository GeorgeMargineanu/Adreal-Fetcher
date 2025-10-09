from .brands_fetcher import BrandFetcher
from .websites_fetcher import PublisherFetcher
from .fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime, timedelta

def return_lookup(data):
    return {str(dat["id"]): dat for dat in data}

def get_brand_owner(brand_id, brands_lookup):
    brand_info = brands_lookup.get(str(brand_id))
    if not brand_info:
        return None
    parent_id = brand_info.get("parent_id")
    if not parent_id:
        return brand_info.get("name")
    parent_info = brands_lookup.get(str(parent_id))
    if not parent_info:
        return None
    return parent_info.get("name")

def decide_content_type(website):
    if not isinstance(website, str) or not website:
        return "Unknown"
    w = website.lower()
    if "google." in w or "bing." in w:
        return "Search"
    if any(social in w for social in ["facebook", "instagram", "tiktok", "youtube"]):
        return "Social"
    return "Standard"

def merge_data(stats_data, brands_data, websites_data):
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)
    all_rows = []

    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_id = segment.get("brand")
        brand_name = brands_lookup.get(str(brand_id), {}).get("name", brand_id)
        brand_owner_name = get_brand_owner(brand_id, brands_lookup)

        product_id = segment.get("product")
        product_name = None
        if product_id and str(product_id) in brands_lookup:
            product_name = brands_lookup[str(product_id)].get("name")

        website_name = websites_lookup.get(str(segment.get("website")), {}).get("name", segment.get("website"))
        content_type = decide_content_type(website_name)

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "product": product_name,
                "website_name": website_name,
                "platform": segment.get("platform"),
                "content_type": content_type,
            }
            # metric values
            for k, v in stat.get("values", {}).items():
                row[k] = v
            # uncertainties
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v
            all_rows.append(row)
    return all_rows

def get_previous_month_range():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    last_prev_month = first_of_current_month - timedelta(days=1)
    first_prev_month = datetime(last_prev_month.year, last_prev_month.month, 1)
    return first_prev_month.strftime("%Y%m%d"), last_prev_month.strftime("%Y%m%d")

def get_previous_month_first_day():
    today = datetime.today()
    first_of_current_month = datetime(today.year, today.month, 1)
    previous_month_last_day = first_of_current_month - timedelta(days=1)
    previous_month_first_day = datetime(previous_month_last_day.year, previous_month_last_day.month, 1)
    return previous_month_first_day.strftime('%Y-%m-01')

def clean_data(df):
    df = df.rename(columns={
        "brand_owner_name": "BrandOwner",
        "brand_name": "Brand",
        "website_name": "MediaChannel",
        "ad_cont": "AdContacts",
        "product": "Product",           
        "content_type": "ContentType",
        "Media owner": "MediaOwner",    
        "Brand owner": "BrandOwner"
    })

    expected_columns = ["Date", "BrandOwner", "Brand", "Product", "ContentType", "MediaOwner", "MediaChannel", "AdContacts"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    df = df[df["MediaChannel"] != "Segment summary"]
    df['Date'] = get_previous_month_first_day()
    df["ContentType"] = df["MediaChannel"].apply(decide_content_type)
    df = df.reindex(columns=expected_columns)
    return df

def run_adreal_pipeline(username, password, market="ro", parent_brand_ids=None):
    if parent_brand_ids is None:
        parent_brand_ids = []

    start_date, end_date = get_previous_month_range()

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
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_data(df)
    return df
