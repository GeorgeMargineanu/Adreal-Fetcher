from google.cloud import secretmanager, bigquery
from gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def push_to_bigquery(df):
    """Stream a DataFrame into BigQuery in batches, with safety checks."""
    client = bigquery.Client()
    table_id = "ums-adreal-471711.Mega.DataImport"

    # Rename columns to match BQ schema
    df = df.rename(columns={
        "Brand owner": "BrandOwner",
        "Brand": "Brand",
        "Product": "Product",
        "Content type": "ContentType",
        "Media channel": "MediaChannel",
        "Ad contacts": "AdContacts",
    })

    # Ensure required columns exist
    required_cols = ["Date", "BrandOwner", "Brand", "ContentType", "MediaChannel", "AdContacts"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Ensure correct types
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime("%Y-%m-%d")
    df["AdContacts"] = pd.to_numeric(df.get("AdContacts"), errors='coerce').fillna(0).astype(int)

    rows_to_insert = df.to_dict(orient="records")
    batch_size = 500
    errors = []
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        errors.extend(client.insert_rows_json(table_id, batch))

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    return f"Streamed {len(df)} rows into {table_id}"

def fetch_adreal_data(request):
    """Cloud Function entry point with robust error handling."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")
        parent_brand_ids = ["13549"]

        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", df.columns)

        period = get_correct_period()

        result = push_to_bigquery(df)
        return f"Data fetched for period {period}: {result}"

    except Exception as e:
        # Log full traceback for debugging
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"
