from google.cloud import secretmanager, bigquery
from common.gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

PROJECT_ID = "ums-adreal-471711"
TABLE_ID = f"{PROJECT_ID}.Muller.DataImport"

def access_secret(secret_id, version_id="latest"):
    """Fetch a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def push_to_bigquery(df):
    """Load DataFrame into BigQuery, replacing only the current month(s)."""
    client = bigquery.Client()

    # Map columns to BigQuery schema
    df = df.rename(columns={
        "Brand owner": "BrandOwner",
        "Brand": "Brand",
        "Content type": "ContentType",
        "Media channel": "MediaChannel",
        "Ad contacts": "AdContacts",
        "Date": "Date"
    })

    # Table Muller.DataImport does NOT have Product
    required_cols = ["Date", "BrandOwner", "Brand", "ContentType", "MediaChannel", "AdContacts"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Keep only required columns in correct order
    df = df[required_cols]

    # Enforce types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df["BrandOwner"] = df["BrandOwner"].astype(str)
    df["Brand"] = df["Brand"].astype(str)
    df["ContentType"] = df["ContentType"].astype(str)
    df["MediaChannel"] = df["MediaChannel"].astype(str)
    df["AdContacts"] = pd.to_numeric(df.get("AdContacts"), errors="coerce").fillna(0).astype(int)

    print("Preview of data to load:")
    print(df.head())

    if df["Date"].isna().all():
        raise ValueError("All dates are missing; cannot load into BigQuery.")

    # Determine months
    months = df["Date"].apply(lambda x: x.replace(day=1)).unique()

    for month in months:
        delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE EXTRACT(YEAR FROM Date) = {month.year}
          AND EXTRACT(MONTH FROM Date) = {month.month}
        """
        print(f"Deleting old rows for {month}")
        client.query(delete_query).result()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()
    return f"Loaded {len(df)} rows into {TABLE_ID} (replacing months: {months})"

def fetch_adreal_data(request):
    """Cloud Function entry point."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # Procredit competitors
        parent_brand_ids = [
            "94444", "17127", "13367", "51367", "11943", "13339", "12681", "37469", "13343", "17986", "94501"
        ]

        # Fetch and process data
        df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
        print("DataFrame fetched. Shape:", df.shape)
        print("Columns:", df.columns)

        # Determine reporting period for logs
        period_date = pd.to_datetime(get_correct_period()[-8:], format="%Y%m%d").strftime("%Y-%m-01")

        # Insert data into BigQuery
        result = push_to_bigquery(df)

        return f"Data fetched for period {period_date}: {result}"

    except Exception as e:
        print("Error occurred:")
        traceback.print_exc()
        return f"Error: {str(e)}\n{traceback.format_exc()}"
