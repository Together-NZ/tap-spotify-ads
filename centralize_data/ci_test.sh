

# Test the dbt-bigquery model accuracy
export DBT_BIGQUERY_DATASET="test_dataset"
export DBT_BIGQUERY_METHOD='service-account'
export DBT_BIGQUERY_PROJECT='together-internal'
export DBT_BIGQUERY_KEYFILE="${DBT_BIGQUERY_KEYFILE:-/path/to/service-account.json}"


grep '^\s*- name: tap-' meltano.yml | sed 's/.*name: //'



for tap in $(grep '^\s*- name: tap-' meltano.yml | sed 's/.*name: //'); do
    meltano invoke "$tap" --help || { echo "$tap failed"; exit 1; }
done


