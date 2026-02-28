# Install pip and meltano
python =m ensurepip --upgrade

pip install meltano 

melano install

# Test the dbt-bigquery compile command
export DBT_BIGQUERY_DATASET="test_dataset"
meltano invoke dbt-bigquery compile

# Somke test the meltano tap
meltano invoke tap-facebook --help || { echo "tap-facebook failed"; exit 1; }

meltano invoke tap-linkedin-ads --help || { echo "tap-linkedin-ads failed"; exit 1; }

meltano invoke tap-ttd --help || { echo "tap-ttd failed"; exit 1; }

meltano invoke tap-dv360 --help || { echo "tap-dv360 failed"; exit 1; }

meltano invoke tap-hivestack --help || { echo "tap-hivestack failed"; exit 1; }