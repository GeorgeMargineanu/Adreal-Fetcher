from google.cloud import secretmanager, bigquery
from gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def push_to_bigquery(df):
    """Delete previous month rows and stream a DataFrame into BigQuery in batches."""
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

    # Ensure Date is string in YYYY-MM-DD format
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    # --- DELETE previous month rows ---
    period_date = df["Date"].iloc[0]  # assuming all rows have the same Date (previous month)
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE Date = '{period_date}'
    """
    client.query(delete_query).result()  # execute and wait for completion

    # Convert to list of dictionaries
    rows_to_insert = df.to_dict(orient="records")

    # Insert in batches
    batch_size = 500
    errors = []
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i + batch_size]
        errors.extend(client.insert_rows_json(table_id, batch))

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    return f"Deleted previous month rows and streamed {len(df)} rows into {table_id}"


def fetch_adreal_data(request):
    """Cloud Function entry point: fetch and load AdReal data."""
    username = access_secret("adreal-username")
    password = access_secret("adreal-password")
    parent_brand_ids = ["13549"]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    period = get_correct_period()

    result = push_to_bigquery(df)

    return f"Data fetched for period {period}: {result}"
