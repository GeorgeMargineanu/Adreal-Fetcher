import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time
import json

class AdRealFetcher:
    def __init__(self, username, password, market="ro", period_range="20250901,20250930,month",
                 brand_ids=None, limit=10000, max_threads=5, target_metric="ad_cont,ru"):
        self.BASE_URL = "https://adreal.gemius.com/api"
        self.LOGIN_URL = f"{self.BASE_URL}/login/?next=/api/"
        self.username = username
        self.password = password
        self.market = market
        self.period_range = period_range
        self.limit = limit
        self.max_threads = max_threads
        self.target_metric = target_metric
        self.session = requests.Session()
        self.platform_id = None
        self.all_results = []
        self.combined_segments = "brand_owner,brand,product,content_type,website,publisher"
        if isinstance(brand_ids, list):
            self.brand_ids = ",".join(brand_ids)
        else:
            self.brand_ids = brand_ids

    # ---------------- LOGIN ----------------
    def login(self):
        login_page = self.session.get(self.LOGIN_URL)
        csrftoken = self.session.cookies.get("csrftoken")
        payload = {
            "username": self.username,
            "password": self.password,
            "csrfmiddlewaretoken": csrftoken
        }
        headers = {"Referer": f"{self.BASE_URL}/{self.market}/stats/", "X-CSRFToken": csrftoken}
        resp = self.session.post(self.LOGIN_URL, data=payload, headers=headers)
        resp.raise_for_status()
        if "invalid" in resp.text.lower():
            raise Exception("Login failed")
        print("Login successful!")

    # ---------------- FETCH OPTIONS ----------------
    def fetch_options(self, endpoint):
        resp = self.session.get(f"{self.BASE_URL}/{self.market}/{endpoint}/")
        resp.raise_for_status()
        return resp.json()

    def get_platform_id(self):
        platforms = self.fetch_options("platforms")["results"]
        self.platform_id = platforms[0]["id"]
        print("Using platform id:", self.platform_id)

    # ---------------- FETCH STATS ----------------
    def fetch_multi_segments(self):
        params_base = {
            "metrics": self.target_metric,
            "platforms": self.platform_id,
            "periods_range": self.period_range,
            "limit": self.limit,
            "mode": "total",
            "brands": self.brand_ids,
            "accumulation_mode": "true",
            "segments": self.combined_segments
        }

        # Fetch first page
        params = params_base.copy()
        params["offset"] = 0
        resp = self.session.get(f"{self.BASE_URL}/{self.market}/stats/", params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        total_count = data.get("total_count", len(results))
        print(f"Total results: {total_count}")

        if total_count <= self.limit:
            self.all_results = results
            return

        # Fetch remaining pages with threading
        offsets = range(self.limit, total_count, self.limit)
        def fetch_page(offset):
            p = params.copy()
            p["offset"] = offset
            r = self.session.get(f"{self.BASE_URL}/{self.market}/stats/", params=p, timeout=120)
            r.raise_for_status()
            return r.json().get("results", [])

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(fetch_page, o) for o in offsets]
            for future in as_completed(futures):
                results.extend(future.result())
        self.all_results = results

    # ---------------- SAVE ----------------
    def save_json(self, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.all_results, f, indent=4, ensure_ascii=False)

    # ---------------- FLATTEN ----------------
    def flatten_to_excel(self, filename):
        all_rows = []

        for item in self.all_results:
            seg_info = item.get("segment", {})
            row = {}
            # Dynamically capture brand_owner, brand, product
            for seg_type, seg_values in seg_info.items():
                if isinstance(seg_values, dict):
                    for k, v in seg_values.items():
                        row[f"{seg_type}_{k}"] = v
                else:
                    row[seg_type] = seg_values

            # Capture stats values and uncertainty
            for stat in item.get("stats", []):
                row_copy = row.copy()
                row_copy["period"] = stat.get("period")
                for k, v in stat.get("values", {}).items():
                    row_copy[k] = v
                for k, v in stat.get("uncertainty", {}).items():
                    row_copy[f"{k}_uncertainty"] = v
                all_rows.append(row_copy)

        df = pd.DataFrame(all_rows)
        df.to_excel(filename, index=False)
        print(f"Saved {len(df)} rows to {filename}")
        print(df.head())
        return df

# ---------------- MAIN ----------------
if __name__ == "__main__":
    start_time = time.time()

    fetcher = AdRealFetcher(
        username="UnitedRO_Teo.Zamfirescu",
        password="TeopassUM25",
        brand_ids = ["88685", "96897", "95300", "96128", "96382", "96382", "97321", "97049", "88599", 
                            "88597", "88586", "53389", "93674", "98190", "97915", "91130", "98006"]
    )

    fetcher.login()
    fetcher.get_platform_id()
    fetcher.fetch_multi_segments()
    fetcher.save_json("adreal.json")
    df = fetcher.flatten_to_excel("adreal.xlsx")

    end_time = time.time()
    print(f"Fetched and saved data in {round((end_time - start_time)/60, 2)} minutes")
