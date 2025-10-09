import requests
import json
import pandas as pd
from urllib.parse import urlencode
from datetime import datetime
import os

# ========================
# CONFIGURATION
# ========================

USERNAME = "UnitedRO_Teo.Zamfirescu"
PASSWORD = "TeopassUM25"
MARKET = "ro"

TEST_BRAND_IDS = ["95300", "91130", "98190", "88586", "53389", "96897", "88685"]
   
PERIOD_RANGE = "20250901,20250930,month"  # previous full month

# Local JSON lookups
FILENAME_BRANDS = "brands.json"
FILENAME_WEBSITES = "websites.json"

# ========================
# FETCHER CLASS
# ========================

class AdRealFetcher:
    def __init__(self, username, password, market="ro", period_range=None):
        self.BASE_URL = "https://adreal.gemius.com/api"
        self.LOGIN_URL = f"{self.BASE_URL}/login/?next=/api/"
        self.username = username
        self.password = password
        self.market = market
        self.period_range = period_range
        self.session = requests.Session()
        self.period_label = self._period_label_from_range(period_range)

    def _period_label_from_range(self, period_range):
        parts = period_range.split(",")
        start = parts[0]
        period_type = parts[2] if len(parts) >= 3 else "month"
        return f"{period_type}_{start}"

    def login(self):
        print("🔐 Logging in...")
        resp = self.session.get(self.LOGIN_URL)
        csrftoken = self.session.cookies.get("csrftoken")
        payload = {
            "username": self.username,
            "password": self.password,
            "csrfmiddlewaretoken": csrftoken,
        }
        headers = {
            "Referer": f"{self.BASE_URL}/{self.market}/stats/",
            "X-CSRFToken": csrftoken,
        }
        r = self.session.post(self.LOGIN_URL, data=payload, headers=headers)
        r.raise_for_status()
        if "invalid" in r.text.lower():
            raise Exception("❌ Login failed")
        print("✅ Login successful")

    def fetch_data(self, brand_ids):
        if isinstance(brand_ids, (list, tuple)):
            brands_param = ",".join(map(str, brand_ids))
        else:
            brands_param = str(brand_ids)

        params = {
            "limit": 1000000,
            "brands": brands_param,
            "format": "json",
            "metrics": "ad_cont",
            "periods_range": self.period_range,
            "platforms": "pc",
            "page_types": "search,social,standard",
            "segments": "brand,product,content_type,website,page_type,platform",
        }

        print("\n🌐 Requesting stats with:")
        print(urlencode(params))
        url = f"{self.BASE_URL}/{self.market}/stats/"
        r = self.session.get(url, params=params, timeout=120)
        r.raise_for_status()
        j = r.json()
        print(f"✅ Got {j.get('total_count', len(j.get('results', [])))} results")
        return j.get("results", [])

# ========================
# HELPERS
# ========================

def read_json(file_path):
    """Read JSON lookup file safely."""
    if not os.path.exists(file_path):
        print(f"⚠️ File not found: {file_path}")
        return []
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

def build_lookup(data):
    """Build a dict of id -> name from JSON list."""
    lookup = {}
    for d in data:
        lookup[d.get("id")] = d.get("name", d.get("label", str(d.get("id"))))
    return lookup

def flatten_results(results, period_label):
    """Flatten raw API results into a DataFrame."""
    all_rows = []
    for item in results:
        segment = item.get("segment", {})
        for stat in item.get("stats", []):
            if stat.get("period") != period_label:
                continue
            row = {}
            # copy segment values
            for seg_type, seg_val in segment.items():
                row[seg_type] = seg_val
            # copy stat values
            for k, v in stat.get("values", {}).items():
                row[k] = v
            all_rows.append(row)
    df = pd.DataFrame(all_rows)
    print(f"📄 Flattened to {len(df)} rows")
    return df

def decide_content_type(website_name):
    """Infer content type if missing."""
    if not isinstance(website_name, str) or not website_name:
        return "Unknown"
    lw = website_name.lower()
    if "google" in lw or "bing" in lw:
        return "Search"
    if any(s in lw for s in ["facebook", "instagram", "youtube", "tiktok"]):
        return "Social"
    return "Standard"

# ========================
# MAIN EXECUTION
# ========================

if __name__ == "__main__":
    # Step 1: Login and fetch
    fetcher = AdRealFetcher(USERNAME, PASSWORD, MARKET, PERIOD_RANGE)
    fetcher.login()
    results = fetcher.fetch_data(TEST_BRAND_IDS)
    df = flatten_results(results, fetcher.period_label)

    # Step 2: Load lookups
    brands_data = read_json(FILENAME_BRANDS)
    websites_data = read_json(FILENAME_WEBSITES)

    brands_lookup = build_lookup(brands_data)
    websites_lookup = build_lookup(websites_data)

        # Step 3: Replace IDs with names
    def safe_str(x):
        try:
            return str(int(x))
        except Exception:
            return str(x)

    # Normalize lookup keys to strings
    brands_lookup = {safe_str(k): v for k, v in brands_lookup.items()}
    websites_lookup = {safe_str(k): v for k, v in websites_lookup.items()}

    if "brand" in df.columns:
        df["Brand"] = df["brand"].astype(str).map(brands_lookup).fillna(df["brand"])
    if "product" in df.columns:
        df["Product"] = df["product"].astype(str).map(brands_lookup).fillna(df["product"])
    if "website" in df.columns:
        df["MediaChannel"] = df["website"].astype(str).map(websites_lookup).fillna(df["website"])
    else:
        df["MediaChannel"] = "Unknown"

    # Step 4: Clean up bad rows
    df = df[~df["Brand"].astype(str).str.contains("Segment summary", case=False, na=False)]
    df = df[~df["Product"].astype(str).str.contains("Segment summary", case=False, na=False)]
    df = df[~df["MediaChannel"].isin(["0", "0.0", "-1", "-1.0", 0, -1])]

    # Step 5: Add inferred content type if missing
    if "content_type" in df.columns:
        df["ContentType"] = df["content_type"]
    else:
        df["ContentType"] = df["MediaChannel"].apply(decide_content_type)

    # Step 6: Add brand owner (same as brand for now)
    df["BrandOwner"] = df["Brand"]

    # Step 7: Select and reorder columns
    export_cols = [
        "BrandOwner",
        "Brand",
        "Product",
        "ContentType",
        "MediaChannel",
        "ad_cont",
    ]
    df = df[[c for c in export_cols if c in df.columns]]

    # Step 8: Final clean-up
    df = df.drop_duplicates()
    df = df.sort_values(by=["BrandOwner", "Product", "MediaChannel"])

    # Step 9: Export
    filename = f"adreal_test_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)
    print(f"💾 Exported clean Excel: {filename}")

    print("\n✅ Sample preview:")
    print(df.head(10))
