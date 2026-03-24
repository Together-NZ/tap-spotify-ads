export DBT_BIGQUERY_DATASET="test_dataset"
export DBT_BIGQUERY_METHOD='oauth'
export DBT_BIGQUERY_PROJECT='together-internal'

meltano install
meltano invoke dbt-bigquery deps
meltano --environment=staging invoke dbt-bigquery compile

CHANNEL_PATTERNS="facebook|dv360|reddit|linkedin|spotify|pinterest|snapchat|tiktok|hivestack|ttd"

COMMANDS=$(grep -E '^\s{6}\w+_models' meltano.yml | sed 's/^[[:space:]]*//' | sed 's/:.*//')

for cmd in $COMMANDS; do
  printf "$cmd\n"
  if echo "$cmd" | grep -qE "$CHANNEL_PATTERNS"; then
    echo "=== Running: meltano --environment=staging invoke dbt-bigquery:$cmd ==="
    meltano --environment=staging invoke "dbt-bigquery:$cmd"
  else
    echo "Skipping $cmd (no channel match)"
  fi
done
meltano --environment=staging invoke dbt-bigquery:test