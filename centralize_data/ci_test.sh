

# Test the dbt-bigquery compile command
export DBT_BIGQUERY_DATASET="test_dataset"
meltano invoke dbt-bigquery compile

# Somke test the meltano tap
#meltano invoke meta --help || { echo "meta failed"; exit 1; }

