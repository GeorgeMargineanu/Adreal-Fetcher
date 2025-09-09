import json
import pandas as pd


class Merger:
    def __init__(self, brands_filename, websites_filename, stats_filename):
        self.brands_filename = brands_filename
        self.websites_filename = websites_filename
        self.stats_filename = stats_filename
        self.all_rows = []

    # --- Utilities ---
    def read_json(self, filename):
        with open(filename, "r", encoding="utf-8") as file:
            return json.load(file)

    def return_lookup(self, data):
        return {dat["id"]: dat["name"] for dat in data}

    # --- Core Merge ---
    def merge(self):
        stats_data = self.read_json(self.stats_filename)
        brands_data = self.read_json(self.brands_filename)
        websites_data = self.read_json(self.websites_filename)

        brands_lookup = self.return_lookup(brands_data)
        websites_lookup = self.return_lookup(websites_data)

        all_rows = []

        for entry in stats_data:
            segment = entry.get("segment", {})
            stats_list = entry.get("stats", [])

            brand_owner_name = brands_lookup.get(
                segment.get("brand_owner"), segment.get("brand_owner")
            )
            brand_name = brands_lookup.get(
                segment.get("brand"), segment.get("brand")
            )
            website_name = websites_lookup.get(
                segment.get("website"), segment.get("website")
            )

            for stat in stats_list:
                row = {
                    "period": stat.get("period"),
                    "brand_owner_name": brand_owner_name,
                    "brand_name": brand_name,
                    "website_name": website_name,
                    "platform": segment.get("platform", None),
                    "content_type": "Standard",  # <-- hardcoded, could be parameterized
                }

                # values
                for k, v in stat.get("values", {}).items():
                    row[k] = v
                # uncertainty
                for k, v in stat.get("uncertainty", {}).items():
                    row[f"{k}_uncertainty"] = v

                all_rows.append(row)

        self.all_rows = all_rows
        return all_rows

    # --- Save ---
    def save_csv(self, filename="mapped_data.csv"):
        if not self.all_rows:
            raise ValueError("No merged rows found. Run merge() first.")
        df = pd.DataFrame(self.all_rows)
        df = df.drop_duplicates()
        df.to_csv(filename, index=False)
        print(f"Saved merged data to {filename} ({len(df)} rows)")


# --- Usage ---
if __name__ == "__main__":
    merger = Merger(
        brands_filename="brands.json",
        websites_filename="publishers.json",
        stats_filename="ad_conts_segments_multi.json",
    )
    merger.merge()
    merger.save_csv("mapped_data.csv")
