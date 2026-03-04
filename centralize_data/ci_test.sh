

# Test the dbt-bigquery compile command
export DBT_BIGQUERY_DATASET="test_dataset"
export DBT_BIGQUERY_AUTH_METHOD='service_account'
export DBT_BIGQUERY_METHOD='service_account'

meltano invoke dbt-bigquery compile

# Somke test the meltano tap
#meltano invoke meta --help || { echo "meta failed"; exit 1; }

