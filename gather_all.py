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
    """Reuses the Merger logic but without files."""
    brands_lookup = return_lookup(brands_data)
    websites_lookup = return_lookup(websites_data)

    all_rows = []

    for entry in stats_data:
        segment = entry.get("segment", {})
        stats_list = entry.get("stats", [])

        brand_owner_name = brands_lookup.get(segment.get("brand_owner"), segment.get("brand_owner"))
        brand_name = brands_lookup.get(segment.get("brand"), segment.get("brand"))
        website_name = websites_lookup.get(segment.get("website"), segment.get("website"))

        for stat in stats_list:
            row = {
                "period": stat.get("period"),
                "brand_owner_name": brand_owner_name,
                "brand_name": brand_name,
                "website_name": website_name,
                "platform": segment.get("platform", None),
                "content_type": "Standard",  # still hardcoded
            }
            # values
            for k, v in stat.get("values", {}).items():
                row[k] = v
            # uncertainty
            for k, v in stat.get("uncertainty", {}).items():
                row[f"{k}_uncertainty"] = v

            all_rows.append(row)

    return all_rows

    def clean_data(self):
        pass


if __name__ == "__main__":
    username = "UnitedRO_Teo.Zamfirescu"
    password = "TeopassUM25"
    market = "ro"
    brand_ids= "13549,701"

    start = time.time()

    #today = datetime.today().strftime("%Y%m%d")
    #period = '_'.join(('month', today))
    period = "month_20250801"

    # --- Fetch all in memory ---
    brand_fetcher = BrandFetcher(username, password, market)
    brand_fetcher.login()
    brands_data = brand_fetcher.fetch_brands(period=period)

    publisher_fetcher = PublisherFetcher(username, password, market)
    publisher_fetcher.login()
    websites_data = publisher_fetcher.fetch_publishers(period=period)

    adreal_fetcher = AdRealFetcher(
        username=username,
        password=password,
        market=market,
        brand_ids=brand_ids  # now it's correctly assigned
    )
    adreal_fetcher.login()
    adreal_fetcher.get_platform_id()
    adreal_fetcher.fetch_multi_segments()
    stats_data = adreal_fetcher.all_results
    
    print("\nMerging all data...")
    # --- Merge everything ---
    merged_rows = merge_data(stats_data, brands_data, websites_data)

    # --- Save only final file ---
    df = pd.DataFrame(merged_rows).drop_duplicates()
    df.to_csv("final_mapped.csv", index=False)
    print(f"\nSaved final merged file with {len(df)} rows")

    end = time.time()
    time_spent = round((end - start), 2)
    print(f"\nThe pipeline took {time_spent} seconds.")
