from brands_fetcher import BrandFetcher
from websites_fetcher import PublisherFetcher
from fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime
import time


def return_lookup(data):
    """Helper: build id -> full brand/publisher info dict."""
    return {dat["id"]: dat for dat in data}


def get_brand_owner(brand_id, brands_lookup):
    """
    Given a brand ID, traverse the parent_id chain to find the top-level Brand owner.
    """
    brand_info = brands_lookup.get(brand_id)
    if not brand_info:
        return None  # brand not found

    parent_id = brand_info.get("parent_id")
    if not parent_id:
        return brand_info["name"]  # this brand is already a top-level owner

    parent_info = brands_lookup.get(parent_id)
    if not parent_info:
        return None  # parent not found

    return parent_info["name"]


def merge_data(stats_data, brands_data, websites_data):
    """Merge stats + brand + websites lookups, filling Brand owner properly."""
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)

    all_rows = []
    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_id = segment.get("brand")
        brand_info = brands_lookup.get(brand_id, {})
        brand_name = brand_info.get("name", brand_id)
        brand_owner_name = get_brand_owner(brand_id, brands_lookup)

        product_name = brands_lookup.get(segment.get("product"), {}).get("name", segment.get("product"))
        website_name = websites_lookup.get(segment.get("website"), {}).get("name", segment.get("website"))

        # Use API-provided content_type if available
        content_type = segment.get("content_type")
        if not content_type or content_type == "None":
            content_type = decide_content_type(website_name)

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "product": product_name,
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
    if 'google.' in lowered_website or 'bing.' in lowered_website:
        return 'Search'
    if any(social in lowered_website for social in ['facebook', 'instagram', 'tiktok', 'youtube']):
        return 'Social'
    return 'Standard'


def clean_data(df):
    columns_to_keep = ["Brand owner", "Brand", "Product", "Content type", "Media channel", "Ad contacts"]

    df = df.rename(columns={
        "brand_owner_name": "Brand owner",
        "brand_name": "Brand",
        "website_name": "Media channel",
        "ad_cont": "Ad contacts",
        "product": "Product"
    })

    # Drop duplicate content_type if present
    if 'content_type' in df.columns and 'Content type' in df.columns:
        df = df.drop('content_type', axis=1)

    # Ensure Content type column exists
    if "Content type" not in df.columns:
        df["Content type"] = df["Media channel"].apply(decide_content_type)

    # Remove summaries from Product
    df = df[df['Media channel'] != 'Segment summary']
    df = df.reindex(columns=columns_to_keep)

    return df


if __name__ == "__main__":
    username = "UnitedRO_Teo.Zamfirescu"
    password = "TeopassUM25"
    market = "ro"
    parent_brand_ids = ["5297", "13549"]   # <-- only parent brands

    start = time.time()
    period = "month_20250801"

    # --- Fetch all in memory ---
    brand_fetcher = BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=period)

    publisher_fetcher = PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=period)

    adreal_fetcher = AdRealFetcher(username=username, password=password, market=market)
    adreal_fetcher.login()

    # Fetch stats for parent brands, expanded by product/content_type/website
    stats_data = adreal_fetcher.fetch_data(
        parent_brand_ids,
        platforms="pc",
        page_types="search,social,standard",
        segments="brand,product,content_type,website",
        limit=1000000
    )

    print("\nMerging all data...")
    merged_rows = merge_data(stats_data, brands_data, websites_data)

    df = pd.DataFrame(merged_rows).drop_duplicates()
    df = clean_data(df)
    df.to_csv(f"{period}_Adreal.csv", index=False)
    print(f"\nSaved final merged file with {len(df)} rows")

    end = time.time()
    print(f"\nThe pipeline took {round(end - start, 2)} seconds.")
