

# Test the dbt-bigquery compile command
export DBT_BIGQUERY_DATASET="test_dataset"
export DBT_BIGQUERY_AUTH_METHOD='service-account'
export DBT_BIGQUERY_METHOD='service-account'
export DBT_BIGQUERY_PROJECT='together-internal'

meltano --environment staging invoke dbt-bigquery:test
grep '^\s*- name: tap-' meltano.yml | sed 's/.*name: //'



for tap in $(grep '^\s*- name: tap-' meltano.yml | sed 's/.*name: //'); do
    meltano invoke "$tap" --help || { echo "$tap failed"; exit 1; }
done


