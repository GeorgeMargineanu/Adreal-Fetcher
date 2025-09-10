from gather_all import run_adreal_pipeline, get_correct_period

def fetch_adreal_data(request):
    """
    Google Cloud Function entry point.
    This function fetches the AdReal data and prints number of rows.
    """
    username = "UnitedRO_Teo.Zamfirescu"
    password = "TeopassUM25"
    parent_brand_ids = ["5297", "13549"]

    df = run_adreal_pipeline(username, password, parent_brand_ids=parent_brand_ids)

    period = get_correct_period()

    # For now, just return number of rows
    return f"Data fetched for period {period}: {len(df)} rows"
