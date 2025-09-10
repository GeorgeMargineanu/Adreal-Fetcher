from brands_fetcher import BrandFetcher
from websites_fetcher import PublisherFetcher
from fetch_adreal import AdRealFetcher
import pandas as pd
from datetime import datetime
import time


def return_lookup(data):
    """Helper: build id→name lookup dict."""
    return {dat["id"]: dat["name"] for dat in data}


def merge_data(stats_data, brands_data, websites_data):
    """Merge stats + brand + websites lookups."""
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)

    all_rows = []
    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_owner_name = brands_lookup.get(segment.get("brand_owner"), segment.get("brand_owner"))
        brand_name = brands_lookup.get(segment.get("brand"), segment.get("brand"))
        product_name = brands_lookup.get(segment.get("product"), segment.get("product"))
        website_name = websites_lookup.get(segment.get("website"), segment.get("website"))

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

    df = df.reindex(columns=columns_to_keep)

    return df


if __name__ == "__main__":
    username = "UnitedRO_Teo.Zamfirescu"
    password = "TeopassUM25"
    market = "ro"
    parent_brand_ids = ["5297", "13549"]   # <--- list of parent brands

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
    df.to_csv("final_mapped.csv", index=False)
    print(f"\nSaved final merged file with {len(df)} rows")

    end = time.time()
    print(f"\nThe pipeline took {round(end - start, 2)} seconds.")
