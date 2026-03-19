export DBT_BIGQUERY_DATASET="test_dataset"
export DBT_BIGQUERY_METHOD='oauth'
export DBT_BIGQUERY_PROJECT='together-internal'

meltano install
meltano invoke dbt-bigquery deps
meltano --environment=staging invoke dbt-bigquery compile
meltano --environment=staging invoke dbt-bigquery:test