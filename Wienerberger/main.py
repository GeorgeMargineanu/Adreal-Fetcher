from google.cloud import secretmanager, bigquery
from common.gather_all import run_adreal_pipeline, get_correct_period
import pandas as pd
import traceback

PROJECT_ID = "ums-adreal-471711"
TABLE_ID = f"{PROJECT_ID}.Wienerberger.DataImport"

def access_secret(secret_id, version_id="latest"):
    """Fetch a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def push_to_bigquery(df):
    """Load DataFrame into BigQuery, replacing only the current month(s)."""
    client = bigquery.Client()

    # Normalize incoming column names → BQ schema
    df = df.rename(columns={
        "Brand owner": "BrandOwner",
        "Brand": "Brand",
        "Product": "Product",
        "Content type": "ContentType",
        "Media channel": "MediaChannel",
        "Ad contacts": "AdContacts",
    })

    # Ensure required columns exist (include Product!)
    required_defaults = {
        "Date": pd.NaT,
        "BrandOwner": None,
        "Brand": None,
        "Product": None,          # <- was missing
        "ContentType": None,
        "MediaChannel": None,
        "AdContacts": 0,
    }
    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default

    # Exclude unwanted brands (works even if values are NaN)
    excluded_brands = ["Agilia", "Chronolia", "Structo Plus"]
    df = df[~df["Brand"].isin(excluded_brands)].copy()
    df = df[~df["Product"].isin(excluded_brands)].copy()

    # Types
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["AdContacts"] = pd.to_numeric(df["AdContacts"], errors="coerce").fillna(0).astype(int)

    # Determine months present, robust to NaT
    months = (
        df["Date"]
        .dt.to_period("M")
        .dropna()
        .unique()
    )

    # Delete old rows for those months (skip if none)
    for p in months:
        delete_query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE EXTRACT(YEAR FROM Date) = {p.year}
          AND EXTRACT(MONTH FROM Date) = {p.month}
        """
        client.query(delete_query).result()

    # Convert Date to DATE (no time) for loading, if your BQ column is DATE
    df["Date"] = df["Date"].dt.date

    # Load new data
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    load_job.result()

    replaced_months_str = ", ".join(str(m) for m in months) if len(months) else "none"
    return f"Loaded {len(df)} rows into {TABLE_ID} (replaced months: {replaced_months_str})"


def fetch_adreal_data(request):
    """Cloud Function entry point."""
    try:
        username = access_secret("adreal-username")
        password = access_secret("adreal-password")

        # Wienerberger competitors
        parent_brand_ids = ["62704", "63564", "31818", "96040", "21067", "37811", "93174", "25456", "36509", "76926", "20216", "21444", "27612", "14387", "28621", "58218", "84106", "52053", 
                            "21327", "89931", "96266", "47648", "20215", "59328", "51584", "88822", "39467", "13381"]
   
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
