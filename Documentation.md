# 🧠 AdReal Fetcher Pipeline — Client Setup Guide

This document explains how to create, configure, and deploy a new AdReal data pipeline for a new client using Google Cloud Functions.

---

## 📁 Repository Structure

Each client (e.g. `Mega`, `Muller`, `ProCredit`) has its own folder under the main repository:

```
Adreal-Fetcher/
├── common/                 # Shared code for all clients (fetchers, utils, etc.)
├── DanoneDairy/
├── Digi/
├── Mega/
│   ├── common/             # Local copy of shared logic (specific to client)
│   ├── main.py             # Entry point for Cloud Function
│   ├── requirements.txt
├── Muller/
├── ProCredit/
└── ...
```

---

## 🧩 How to Add a New Client

### Step 1. Copy an Existing Client Folder

Pick a similar existing client (e.g. `Mega`) and duplicate it:

```bash
cp -r Mega NewClient
```

You’ll now have:

```
NewClient/
├── common/
├── main.py
├── requirements.txt
```

---

### Step 2. Edit `main.py`

Inside `main.py`, locate the line defining `parent_brand_ids` in `fetch_adreal_data`:

```python
parent_brand_ids = [
    "94444", "17127", "13367", "157", "51367", "11943"
]
```

Replace the IDs with those specific to the new client.

Inside `main.py`, locate the line 
```python
TABLE_ID = f"{PROJECT_ID}.Client.DataImport"
```
Replace 'Client' with the correct Client Name (Muller, Mega...)

⚠️Any change inside the specific code for each client requires redeployment.
---

### Step 3. Deploy the Cloud Function

From within the new client folder (`cd NewClient`), deploy the function:

```bash
gcloud functions deploy fetch_adreal_newclient   --region=europe-west1   --runtime=python310   --trigger-http   --allow-unauthenticated   --memory=512MB   --timeout=120s   --source=.   --entry-point=fetch_adreal_data   --gen2   --service-account=ums-adreal-471711@appspot.gserviceaccount.com
```

This creates a Cloud Function that:

- Fetches data from the AdReal API  
- Cleans and formats it  
- Pushes to BigQuery (`ums-adreal-471711`)  
- Uses secrets from Secret Manager for credentials  

---

### Step 4. Scheduling

Each client function is triggered automatically on the **3rd of every month** in the morning (configured via **Cloud Scheduler**).

**Example Cloud Scheduler setup:**  
- **Schedule:** `0 7 3 * *` → runs at 07:00 UTC on the 3rd day of each month  
- **Target:** HTTPS URL of the deployed Cloud Function  

---

### 🔐 Secrets & Permissions

**Stored in:** Google Secret Manager  
- `adreal-username`  
- `adreal-password`  

**Service Account:** `ums-adreal-471711@appspot.gserviceaccount.com`  
Has roles:  
- Secret Manager Accessor  
- BigQuery Admin  
- Cloud Functions Invoker  

---

## ⚙️ Manual Push Command

To manually push data for a specific month, run this command from the repository root:

```bash
python -m common.manual_push_to_bq 2025 8
```

> ⚠️ **CRITICAL WARNING:**  
> The manual push script (`manual_push_to_bq`) uses a Replace-by-Month ingestion strategy.  
> Any existing data in the BigQuery destination table for the specified month **WILL BE DELETED** and replaced with new data fetched from the AdReal API.  
> Use with caution.
