from google.cloud import bigquery
from google.cloud import storage
import re
from io import StringIO
import pandas as pd


project_id = "arvida-main"
dataset_id = "neilson_raw"
table_id = "neilson_staging"
bucket_name = "adintel-cleaned"
prefix = "arvida/"
date_pattern = r"Arvida_Compete_Report_(\d{8})\.csv"

bq = bigquery.Client(project=project_id)
gcs = storage.Client(project=project_id)

def sync_date_to_gcs(bucket_name, blob_path, latest_date_str):
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(latest_date_str)
    print(f"📝 Wrote latest date to gs://{bucket_name}/{blob_path}")

def read_latest_date_from_gcs(bucket_name, blob_path):
    bucket = gcs.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    try:
        latest_date = int(blob.download_as_string())
        print(f"📤 Latest processed date in gs://{bucket_name}/{blob_path}: {latest_date}")
    except Exception as e:
        print(f"⚠️ Couldn't read last sync date: {e}")
        latest_date = None
    return latest_date

def list_csv_files():
    bucket = gcs.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if re.search(date_pattern, blob.name)]

def extract_date_from_filename(filename):
    match = re.search(date_pattern, filename)
    return match.group(1) if match else None

def main():
    state_bucket = "arvida-main-meltano-state-staging"
    state_blob_path = "neilson_latest_date"
    latest_synced_date = read_latest_date_from_gcs(state_bucket, state_blob_path)

    files = list_csv_files()
    print(f"✅ Found {len(files)} matching CSV files")

    merged_df = pd.DataFrame()
    latest_processed_date = latest_synced_date

    for file in sorted(files):
        date_str = extract_date_from_filename(file)
        if not date_str:
            continue
        if latest_synced_date and int(date_str) <= int(latest_synced_date):
            print(f"⏭️ Skipping already-processed file: {file}")
            continue

        print(f"📥 Processing file: {file}")
        blob = gcs.bucket(bucket_name).blob(file)
        content = blob.download_as_text()
        df = pd.read_csv(StringIO(content))
        df.columns = df.columns.str.replace(r"[^\w]", "_", regex=True)
        df["report_date"] = pd.to_datetime(date_str, format="%Y%m%d")

        merged_df = pd.concat([merged_df, df], ignore_index=True)

        # Track latest date processed
        if not latest_processed_date or int(date_str) > int(latest_processed_date):
            latest_processed_date = date_str

    if merged_df.empty:
        print("📭 No new data to load.")
        return

    print(f"🚀 Uploading {len(merged_df)} rows to BigQuery...")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(
        merged_df,
        f"{project_id}.{dataset_id}.{table_id}",
        job_config=job_config
    )
    job.result()

    print(f"✅ Upload successful. Latest date processed: {latest_processed_date}")
    sync_date_to_gcs(state_bucket, state_blob_path, latest_processed_date)

if __name__ == "__main__":
    main()
