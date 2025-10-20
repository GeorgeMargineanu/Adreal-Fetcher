from gather_all import run_adreal_pipeline, get_correct_period
from google.cloud import secretmanager

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"  # your actual project id
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def main():
    username = access_secret("adreal-username")
    password = access_secret("adreal-password")
    # ProCredit Competitors
    parent_brand_ids = [
    "94443", "159", "158", "14167", "14174", "13344", "23697", "696", "12684", "17607", "67048", "94272", 
    "94494", "93768", "93812", "94489", "94490", "94214", "94484", "94554", "43854", "94570",# Grija la carpos
    "94553", "92370", "94542", "94568", "94569", "94550", "17128", "30946", "97254", "94552",
    "94540"
]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    print(f"Data fetched: {len(df)} rows")

    period = get_correct_period()
    df.to_csv(f"{period}_Adreal.csv", index=False)

if __name__ == "__main__":
    main()
