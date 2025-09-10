from gather_all import run_adreal_pipeline, get_correct_period

def main():
    username = ""
    password = ""
    parent_brand_ids = ["5297", "13549"]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    #df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()
