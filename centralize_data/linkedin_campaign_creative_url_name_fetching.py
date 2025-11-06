# enrich_creatives.py

from google.cloud import bigquery
import pandas as pd
from linkedin_name_fetching import Parsed_campaign_creative_click_url_name
import time

# Initialize BigQuery client

client = bigquery.Client(project="together-internal")
# Caching repeated creative lookups
creative_cache = {}

def enrich_row(row):
    creative_id = row["creative_id"]
    ad_account_id=row["advertiser_account_id"]
    if not creative_id:
        return row

    if creative_id in creative_cache:
        creative_name, campaign_name, click_url = creative_cache[creative_id]
    else:
        try:
            campaign_name, creative_name,click_url = Parsed_campaign_creative_click_url_name.get_creative_data(ad_account_id,creative_id)
            creative_cache[creative_id] = (creative_name, campaign_name, click_url)
            print(creative_name)
            time.sleep(0.2)  # Throttle if you're hitting an API
        except Exception as e:
            print(f"Error enriching {creative_id}: {e}")
            return row  # Return unmodified row if failed

    row["creative_name"] = creative_name
    row["campaign_name"] = campaign_name
    row["click_url"] = click_url
    return row

def main():
    Parsed_campaign_creative_click_url_name.initialise_access_token("LINKEDIN_ACCESS_TOKEN")
    # Step 1: Read from BigQuery
    query = """
        SELECT distinct SPLIT(json_value(data,'$.account'),':')[OFFSET(3)] AS advertiser_account_id,
        SPLIT(json_value(data,'$.id'),':')[OFFSET(3)] AS creative_id
        FROM `together-internal.linkedin_raw.creatives` 

    """
    df = client.query(query).to_dataframe()

    # Step 2: Enrich
    df = df.apply(enrich_row, axis=1)


    # Step 3: Save back to BigQuery
    table_id = "together-internal.linkedin_transformed.linkedin_naming"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print("✅ Enriched data uploaded to BigQuery.")

if __name__ == "__main__":
    main()
