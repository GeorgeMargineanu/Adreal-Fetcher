import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


class BrandFetcher:
    def __init__(self, username, password, market="ro", max_threads=5, limit=100000):
        self.BASE_URL = "https://adreal.gemius.com/api"
        self.LOGIN_URL = f"{self.BASE_URL}/login/?next=/api/"
        self.username = username
        self.password = password
        self.market = market
        self.limit = limit
        self.max_threads = max_threads
        self.session = requests.Session()
        self.all_brands = []

    # ---------------- LOGIN ----------------
    def login(self):
        print('\nStarted getting Brands data.')
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

    def add_other_children(self, brands, start_id=10000000):
        """
        For each ROOT brand (parent_id is None), add a single child named 'Other'
        IFF such a child doesn't already exist. IDs start at start_id and avoid collisions.
        """
        # Existing IDs (avoid collisions)
        existing_ids = [b.get("id") for b in brands if "id" in b]
        max_existing = max(existing_ids) if existing_ids else 0
        next_id = max(start_id, max_existing + 1)

        # Build quick lookup: parent_id -> list of children
        from collections import defaultdict
        children_by_parent = defaultdict(list)
        for b in brands:
            pid = b.get("parent_id")
            if pid is not None:
                children_by_parent[pid].append(b)

        roots = [b for b in brands if b.get("parent_id") is None]

        new_entries = []
        for root in roots:
            # check if an 'Other' child already exists (case-insensitive)
            existing_other = any(
                (child.get("name") or "").strip().lower() == "other"
                for child in children_by_parent.get(root["id"], [])
            )
            if existing_other:
                continue

            new_entries.append({
                "id": next_id,
                "encrypted_id": None,
                "parent_id": root["id"],
                "name": "Other",
            })
            next_id += 1

        if new_entries:
            print(f"Added {len(new_entries)} synthetic 'Other' children.")
        else:
            print("No synthetic 'Other' children added (all roots already had one).")

        return brands + new_entries


    # ---------------- FETCH ----------------
    def fetch_brands(self, period):
        """Fetch all brands for a given period (handles pagination with threads)."""
        # Initial request
        resp = self.session.get(
            f"{self.BASE_URL}/{self.market}/brands/",
            params={"period": period, "limit": self.limit, "offset": 0},
        )
        resp.raise_for_status()
        data = resp.json()
        total_count = data.get("total_count", len(data.get("results", [])))
        print(f"Total brands to fetch for {period}: {total_count}")

        # Prepare offsets
        offsets = list(range(0, total_count, self.limit))

        def fetch_page(offset):
            params = {"period": period, "limit": self.limit, "offset": offset}
            r = self.session.get(
                f"{self.BASE_URL}/{self.market}/brands/", params=params, timeout=30
            )
            r.raise_for_status()
            results = r.json().get("results", [])
            print(f"Fetched {len(results)} brands at offset {offset}")
            return results

        # Fetch all concurrently
        results = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(fetch_page, o) for o in offsets]
            for future in as_completed(futures):
                results.extend(future.result())

        results = self.add_other_children(results)
        self.all_brands = results
        print(f"Done! Fetched {len(results)} brands for {period}")
        return results
    
    # ---------------- SAVE ----------------
    def save_json(self, filename="brands.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.all_brands, f, indent=4, ensure_ascii=False)
        print(f"Saved JSON to {filename}")

    def save_csv(self, filename="brands.csv"):
        pd.DataFrame(self.all_brands).to_csv(filename, index=False)
        print(f"Saved CSV to {filename}")


# ---------------- MAIN ----------------
if __name__ == "__main__":
    fetcher = BrandFetcher(
        username = "",
        password = "",
        market="ro",
    )
    fetcher.login()
    fetcher.fetch_brands(period="month_20250801")
    fetcher.save_json("brands.json")
    #fetcher.save_csv("brands.csv")
