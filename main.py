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


def push_to_bigquery(df, period_date):
    """Load a DataFrame into BigQuery, overwriting the target partition."""
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

    # Configure load job to overwrite the partition for that Date
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(field="Date")
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    return f"Loaded {len(df)} rows into {table_id} for period {period_date}"


def fetch_adreal_data(request):
    """Cloud Function entry point with partition overwrite and robust error handling."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # Mega competitors
        parent_brand_ids = [
            "13549", "93773", "10566", "49673", "695",
            "12968", "16238", "701", "688", "8196",
            "89922", "704", "97637", "93160"
        ]

        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", df.columns)

        # Determine correct period (set to first day of month)
        period_date = pd.to_datetime(get_correct_period()[-8:], format='%Y%m%d').strftime('%Y-%m-01')

        # Insert fresh data (overwrites only that partition)
        result = push_to_bigquery(df, period_date)

        return f"Data fetched for period {period_date}: {result}"

    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"
