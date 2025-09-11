from google.cloud import secretmanager, bigquery
from gather_all import run_adreal_pipeline, get_correct_period

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def push_to_bigquery(df):
    """Push the dataframe into BigQuery."""
    client = bigquery.Client()
    table_id = "ums-adreal-471711.Mega.DataImport"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Append new rows
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait until the job completes

    return f"Loaded {len(df)} rows into {table_id}"

def fetch_adreal_data(request):
    """
    Google Cloud Function entry point.
    Fetches AdReal data, cleans it, and loads into BigQuery.
    """
    username = access_secret("adreal-username")
    password = access_secret("adreal-password")
    parent_brand_ids = ["13549"]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    period = get_correct_period()

    result = push_to_bigquery(df)

    return f"Data fetched for period {period}: {len(df)} rows. {result}"
