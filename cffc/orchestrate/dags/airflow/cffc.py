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


IMAGE = "australia-southeast1-docker.pkg.dev/cffc-main/meltano/meltano-cffc-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2026, 3, 5, tzinfo=local_tz)
}
dv360_args = {
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=30),
    "start_date": yesterday,
    "catchup": False,
    "concurrency": 1,
    "max_active_runs": 1
}

# Setting timezone for DAG's start date
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)
start_date_str = start_date.strftime("%Y-%m-%d")
start_date_str = yesterday.strftime("%Y-%m-%d")
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_cffc_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy

with models.DAG(
    dag_id="cffc-meltano-extraction-transformation-dbt",
    schedule_interval="0 1 * * *",
    default_args=default_args,
) as dag:
    def set_env_vars_linkedin(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'linkedin_transformed__{label}'
        return env
    def set_env_vars_snapchat(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "snapchat_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'snapchat_transformed__{label}'
        return env
    def set_env_vars_facebook(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
        return env
    def set_env_vars_dv360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
        return env

    def set_env_vars_ttd(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
        return env
    def set_env_vars_dash_union(list):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{list}'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__retirement_commission'
        return env
    #for key_id,value in {env['TAP_GA4_PROPERTY_ID']: 'GA4',}
    def set_env_vars_ga4(id,value):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw__{value}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{value}'   
        env["TAP_GA4_PROPERTY_ID"] = id    
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        env["TAP_GA4_START_DATE"] = ga4_start_date_str
        return env
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'cffc-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    label_kube_list = {}
    dash_search_list = {}
    label_kube_ga4_list = {}
    env = get_meltano_env()
    ga4_List = {env["TAP_GA4_PROPERTY_ID"]: "retirement_commission",env["TAP_GA4_PROPERTY_ID_SORTED"]: "sorted",env["TAP_GA4_PROPERTY_ID_SORTED_IN_SCHOOL"]: "sorted_in_school",
                env["TAP_GA4_PROPERTY_ID_SMART_SORTED"]:"smart_sorted"}
    for account_id,label in ga4_List.items():
        kube_ga4 = KubernetesPodOperator(
            name=f"cffc-ga4-to-bigquery-{label}",
            task_id=f"cffc-ga4_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(account_id,label),
        )
        #label_kube_ga4_list.setdefault(label, []).append(kube_ga4)
    list = ["cffc"]
    for label in list:
        kube_linkedin = KubernetesPodOperator(
            name=f"cffc-linkedin-to-bigquery-{label}",
            task_id=f"cffc-linkedin_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",f"dbt-bigquery:linkedin_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars = set_env_vars_linkedin(label),
        )
        label_kube_list.setdefault(label, []).append(kube_linkedin)
        kube_cm360 = KubernetesPodOperator(
            name=f"cffc-cm360-transformation-{label}",
            task_id = f"cffc-cm360_transformation_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{label}_models"],
            env_vars = set_env_vars_cm360(label),
        )
        label_kube_list.setdefault(label, []).append(kube_cm360)
        kube_snapchat = KubernetesPodOperator(
            name=f"cffc-snapchat-to-bigquery-{label}",
            task_id=f"cffc-snapchat_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery",f"dbt-bigquery:snapchat_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            )
            ,env_vars=set_env_vars_snapchat(label)
        )
        label_kube_list.setdefault(label, []).append(kube_snapchat)
        kube_facebook = KubernetesPodOperator(
            name=f"cffc-facebook-to-bigquery-{label}",
            task_id=f"cffc-facebook_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(label),
            #base_container_name=f"meltano-cffc-facebook",
        )
        label_kube_list.setdefault(label, []).append(kube_facebook)
        kube_dv360 = KubernetesPodOperator(
            name=f"cffc-dv360-to-bigquery-{label}",
            task_id=f"cffc-dv360_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",f"dbt-bigquery:dv360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(label),
            get_logs=True
        )
        label_kube_list.setdefault(label, []).append(kube_dv360)
        kube_ttd = KubernetesPodOperator(
            name=f"cffc-ttd-to-bigquery-{label}",
            task_id=f"cffc-ttd_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery",f"dbt-bigquery:ttd_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(label),
            base_container_name=f"meltano-cffc-ttd",
        )
        label_kube_list.setdefault(label, []).append(kube_ttd)
        kube_cm360 >> kube_ttd
    search_list = ["sorted","sorted_in_school"]
    for label in search_list: 
        kube_google_ads_search = KubernetesPodOperator(
            name=f"cffc-google-ads-search-to-bigquery-{label}",
            task_id=f"cffc-google-ads-search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"google_ads_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars = set_env_vars_google_ads_search(label),
        )
        kube_dash_search = KubernetesPodOperator(
                name=f"cffc-dash-search-to-bigquery-{label}",
                task_id=f"cffc-dash-search_to_bigquery-{label}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{label}"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars = set_env_vars_dash_search(label),
            )
        kube_google_ads_search>> kube_dash_search
        dash_search_list.setdefault(label, []).append(kube_dash_search)
    overall_list = ['cffc','sorted','sorted_in_school']
    for list in overall_list:
        if list == "cffc":
            real_name = "retirement_commission"
        else:
            real_name = list
        kube_dash_union = KubernetesPodOperator(
                name=f"cffc-dash-union-to-bigquery-{real_name}",
                task_id=f"cffc-dash_union_to_bigquery-{real_name}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{real_name}"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars = set_env_vars_dash_union(real_name),
            )
        if list != "cffc":
            for task in dash_search_list[list]:
                task >> kube_dash_union
        if list == "cffc":
            name='retirement_commission'
            kube_dash = KubernetesPodOperator(
            name=f"cffc-dash-to-bigquery-{name}",
            task_id=f"cffc-dash_to_bigquery-{name}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__retirement_commission"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
            trigger_rule = 'all_done'
            )
            for upstream in label_kube_list[list]:
                upstream >> kube_dash
            kube_dash >> kube_dash_union
