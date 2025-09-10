from google.cloud import secretmanager
from gather_all import run_adreal_pipeline, get_correct_period

def access_secret(secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "ums-adreal-471711"  # your actual project id
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def fetch_adreal_data(request):
    """
    Google Cloud Function entry point.
    Fetches AdReal data using secrets.
    """
    username = access_secret("adreal-username")
    password = access_secret("adreal-password")
    parent_brand_ids = ["5297", "13549"]  # could also be a secret if needed

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)
    period = get_correct_period()

    return f"Data fetched for period {period}: {len(df)} rows"
