from google.cloud import secretmanager, bigquery
from gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

def access_secret(secret_id, version_id="latest"):
    """Fetch a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def delete_previous_month(client, table_id, period_date):
    """Delete rows for a specific month to prevent duplicates."""
    query = f"""
    DELETE FROM `{table_id}`
    WHERE Date = '{period_date}'
    """
    print(f"Deleting rows for Date = {period_date} ...")
    client.query(query).result()
    print("Deletion complete.")

def push_to_bigquery(df):
    """Stream a DataFrame into BigQuery in batches."""
    client = bigquery.Client()
    table_id = "ums-adreal-471711.Mega.DataImport"

    # Rename columns to match BigQuery schema
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

    # Insert in batches
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
    """Cloud Function entry point with pre-delete and robust error handling."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")
        parent_brand_ids = ["13549","10566"]

        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", df.columns)

        period_date = pd.to_datetime(get_correct_period()[-8:], format='%Y%m%d').strftime('%Y-%m-01')

        # Initialize BigQuery client
        client = bigquery.Client()
        table_id = "ums-adreal-471711.Mega.DataImport"

        # Delete previous month to avoid duplicates
        delete_previous_month(client, table_id, period_date)

        # Insert fresh data
        result = push_to_bigquery(df)

        return f"Data fetched for period {period_date}: {result}"

    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"
