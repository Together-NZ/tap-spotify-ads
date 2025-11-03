import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import sys
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import json
import time
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import secretmanager
from google.cloud import storage 
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
import json


IMAGE = "australia-southeast1-docker.pkg.dev/curative-main/meltano/meltano-curative-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)
all_tasks = []
per_label_task = {}
local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": yesterday
}
dv360_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=3),
    "start_date": yesterday,
    "catchup": False,
    "concurrency": 1,
    "max_active_runs": 1
}

# Setting timezone for DAG's start date
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)
start_date_str = start_date.strftime("%Y-%m-%d")
start_date_str = yesterday.strftime("%Y-%m-%d")
today_date_str = datetime.datetime.now(local_tz).strftime("%Y-%m-%d")
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_curative_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy

with models.DAG(
    dag_id="curative-meltano-extraction-transformation-dbt",
    schedule_interval="0 1 * * *",
    default_args=default_args,
) as dag:
    env = get_meltano_env()

        
    def set_env_vars_hivestack(id,label):

        env = get_meltano_env()
        env["BQ_DATASET"] = f"hivestack_{label}_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'hivestack_{label}_transformed'
        env["TAP_HIVESTACK_REPORT_ID"] = id
        return env      
    def set_env_vars_snapchat(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"snapchat_{label}_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'snapchat_{label}_transformed'
        env["TAP_SNAPCHAT_ADS_AD_ACCOUNT_IDS"]=id
        return env
    def set_env_vars_facebook(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_{label}_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_{label}_transformed'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"]=id
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_{label}_transformed'
        return env
    def set_env_vars_dv360(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_{label}_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_{label}_transformed'
        env["DV360_ADVERTISER_ID"] = id
        return env

    def set_env_vars_ttd(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ttd_{label}_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_{label}_transformed'
        env["TAP_TTD_ADVERTISER_ID"] = id
        return env
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_{label}_search_transformed'
        return env
    def set_env_vars_dash_table_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_tiktok(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"tiktok_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed__{label}'
        env["TAP_TIKTOK_ADVERTISER_ID"] = id
        return env
    def set_env_vars_ga4(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_{label}_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'curative-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'       
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        env["TAP_GA4_PROPERTY_ID"] = id
        env["TAP_GA4_START_DATE"] =ga4_start_date_str
        env["TAP_GA4_END_DATE"] = today_date_str
        return env
    env = get_meltano_env()
    snapchat_list = {env["TAP_SNAPCHAT_PROTECTYOURBREATH_ID"]:'protectyourbreath'}
    for id, label in snapchat_list.items():
        kube_snapchat = KubernetesPodOperator(
            name = f'curative-{label}-snapchat-to-bigquery',
            task_id = f'curative-{label}-snapchat_to_bigquery',
            namespace = 'composer-user-workloads',
            image = IMAGE,
            arguments = ["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery",f"dbt-bigquery:snapchat_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_snapchat(id,label),
            #base_container_name = f"meltano-{label}-snapchat",
            get_logs = True
        )
        all_tasks.append(kube_snapchat)
        per_label_task.setdefault(label, []).append(kube_snapchat)
    hivestack_list = {env["TAP_HIVESTACK_PROTECTYOURBREATH_ID"]:'protectyourbreath'}
    for id, label in hivestack_list.items():
        kube_hivestack = KubernetesPodOperator(
            name=f"curative-{label}-hivestack-to-bigquery",
            task_id=f"curative-{label}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",f"dbt-bigquery:hivestack_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(id,label),
            #base_container_name=f"meltano-{label}-hivestack",
            get_logs = True
        )
        all_tasks.append(kube_hivestack)
        per_label_task.setdefault(label, []).append(kube_hivestack)
    tiktok_list = {env["TAP_TIKTOK_PROTECTYOURBREATH_ID"]:'protectyourbreath',env["TAP_TIKTOK_CREATIVENZ_ID"]:'creativenz',
                   env["TAP_TIKTOK_MAHI_ID"]:'mahi'}
    for id,label in tiktok_list.items():
        kube_tiktok = KubernetesPodOperator(
            name=f"curative-{label}-tiktok-to-bigquery",
            task_id=f"curative-{label}-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery",f"dbt-bigquery:tiktok_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(id,label),
            #base_container_name=f"meltano-{label}-tiktok",
            get_logs = True
            )
        all_tasks.append(kube_tiktok)
        per_label_task.setdefault(label, []).append(kube_tiktok)
    ga4_list = {env["TAP_GA4_FASD_ID"]:'fasd',env["TAP_GA4_REALNURSES_ID"]:'realnurses',env["TAP_GA4_MAHI_ID"]:'mahi'}
    for id, label in ga4_list.items():
        
        kube_ga4 = KubernetesPodOperator(
            name=f"curative-{label}-ga4-to-bigquery",
            task_id=f"curative-{label}-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(id,label),
            #base_container_name=f"meltano-{label}-ga4",
            get_logs = True
        )
        all_tasks.append(kube_ga4)
        per_label_task.setdefault(label, []).append(kube_ga4)
    ttd_list = {env["TAP_CREATIVENZ_TTD_ID"]:'creativenz',env["TAP_FASD_TTD_ID"]:'fasd',
                env["TAP_MAHI_TTD_ID"]:'mahi',env["TAP_REALNURSES_TTD_ID"]:'realnurses',env['TAP_PROTECTYOURBREATH_TTD_ID']:'protectyourbreath'}
    for id, label in ttd_list.items():
        kube_ttd = KubernetesPodOperator(
            name=f"curative-{label}-ttd-to-bigquery",
            task_id=f"curative-{label}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery",f"dbt-bigquery:ttd_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(id,label),
            #base_container_name=f"meltano-{label}-ttd",
            get_logs = True
        )
        all_tasks.append(kube_ttd)
        per_label_task.setdefault(label, []).append(kube_ttd)
        if label !='realnurses' and label !='mahi':
            kube_cm360 = KubernetesPodOperator(
                name=f"curative-{label}-cm360-transformation",
                task_id = f"curative-{label}-cm360_transformation",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{label}_models"],
                env_vars = set_env_vars_cm360(label),
                get_logs = True
            )
            all_tasks.append(kube_cm360)
            per_label_task.setdefault(label, []).append(kube_cm360)
            kube_cm360 >> kube_ttd
    facebook_list = {env["TAP_FACEBOOK_CREATIVE_ID"]:'creativenz',env["TAP_FACEBOOK_FASD_ID"]:'fasd',env['TAP_FACEBOOK_PROTECTYOURBREATH_ID']:'protectyourbreath'}
    for id, label in facebook_list.items():
        kube_facebook = KubernetesPodOperator(
            name=f"curative-{label}-facebook-to-bigquery",
            task_id=f"curative-{label}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(id,label),
            #base_container_name=f"meltano-{label}-facebook",
            get_logs = True
        )
        all_tasks.append(kube_facebook)
        per_label_task.setdefault(label, []).append(kube_facebook)
    dv360_list = {env["DV360_FASD_ID"]:'fasd',env["DV360_MAHI_ID"]:'mahi',env["DV360_PROTECTYOURBREATH_ID"]:'protectyourbreath'}
    for id, label in dv360_list.items():
        kube_dv360 = KubernetesPodOperator(
            name=f"curative-{label}-dv360-to-bigquery",
            task_id=f"curative-{label}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",f"dbt-bigquery:dv360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(id,label),
            #base_container_name=f"meltano-{label}-dv360",
            get_logs = True
        )
        all_tasks.append(kube_dv360)
        per_label_task.setdefault(label, []).append(kube_dv360)

    all_labels = set([
        *facebook_list.values(),
        *dv360_list.values(),
        *ttd_list.values(),
        *ga4_list.values(),
        *hivestack_list.values(),
    ])
    for label in all_labels:
        kube_dash_union = KubernetesPodOperator(
            name=f"curative-{label}-dash-union-to-bigquery",
            task_id=f"curative-{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            )
        
        kube_dash = KubernetesPodOperator(
            name=f"curative-{label}-dash-to-bigquery",
            task_id=f"curative-{label}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            trigger_rule = 'all_done',
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
        )
        for upstream_task in per_label_task.get(label,[]):
            upstream_task >> kube_dash
        kube_google_ads_search  = KubernetesPodOperator(
            name = f'curative-{label}-google-ads-search-to-bigquery',
            task_id = f'curative-{label}-google-ads-search_to_bigquery',
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            #base_container_name=f"meltano-{label}-google-ads-search",
        )
        kube_dash_table_search = KubernetesPodOperator(
            name = f'curative-{label}-dash-table-search-to-bigquery',
            task_id = f'curative-{label}-dash-table-search_to_bigquery',
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_table_search(label),
            #base_container_name=f"meltano-{label}-dash-table-search",
        )
        kube_google_ads_search >> kube_dash_table_search
        kube_dash >> kube_dash_table_search >> kube_dash_union


    