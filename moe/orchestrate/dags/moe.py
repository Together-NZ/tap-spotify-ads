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


IMAGE = "australia-southeast1-docker.pkg.dev/moe-main/meltano/meltano-moe-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
ga4_start_date = datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
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
    meltano_env_unique = Variable.get("meltano_moe_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy

with models.DAG(
    dag_id="moe-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:   
    def set_env_vars_google_ads_search():
        env= get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_snapchat():
        env = get_meltano_env()
        env["BQ_DATASET"] = "snapchat_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'snapchat_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
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
    def set_env_vars_ga4_merge():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        return env

    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_ga4(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw__{label}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'moe-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'       
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env['TAP_GA4_START_DATE'] = ga4_start_date_str
        env['TAP_GA4_PROPERTY_ID']=id
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env

    set_env_task_google_ads_search = PythonOperator(
        task_id="set_env_google_ads_search",
        python_callable=set_env_vars_google_ads_search,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_dash_search",
        python_callable=set_env_vars_dash_search,
    )
    set_env_task_tiktok = PythonOperator(
        task_id="set_env_tiktok",
        python_callable=set_env_vars_tiktok,
    )
    set_env_task_ga4_merge = PythonOperator(
        task_id="set_env_ga4_merge",
        python_callable=set_env_vars_ga4_merge,
    )
    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_linkedin = PythonOperator(
        task_id="set_env_linkedin",
        python_callable=set_env_vars_linkedin,
    )
    set_env_task_snapchat = PythonOperator(
        task_id="set_env_snapchat",
        python_callable=set_env_vars_snapchat,
    )
    set_env_task_dv360 = PythonOperator(
        task_id="set_env_dv360",
        python_callable=set_env_vars_dv360,
    )
    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_ttd = PythonOperator(
        task_id="set_env_ttd",
        python_callable=set_env_vars_ttd,
    )
  
    kube_linkedin = KubernetesPodOperator(
        name="moe-linkedin-to-bigquery",
        task_id="moe-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments = [
             "--environment=prod", "invoke","dbt-bigquery:linkedin_models"],
        env_vars=set_env_vars_linkedin(),
        
    )
    kube_google_ads_search = KubernetesPodOperator(
        name="moe-google-ads-search-to-bigquery",
        task_id="moe-google-ads-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","google_ads_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_search()
    )

    kube_tiktok = KubernetesPodOperator(
        name="moe-tiktok-to-bigquery",
        task_id="moe-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok()
    )
    env = get_meltano_env()
    ga4_list_task = {}
    ga4_list = {env["TAP_GA4_PROPERTY_ID_CAREER"]:'career',env["TAP_GA4_PROPERTY_ID_EDUCATION"]:'education'}
    for id,name in ga4_list.items():
        kube_ga4 = KubernetesPodOperator(
            name="moe-ga4-to-bigquery-"+name,
            task_id="moe-ga4_to_bigquery_"+name,
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{name}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(id,name)
        )
        ga4_list_task.setdefault(name,[]).append(kube_ga4)
        
    kube_facebook = KubernetesPodOperator(
        name="moe-facebook-to-bigquery",
        task_id="moe-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook()
    )
    kube_snapchat = KubernetesPodOperator(
        name="moe-snapchat-to-bigquery",
        task_id="moe-snapchat_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery","dbt-bigquery:snapchat_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_snapchat()
    )
    kube_dv360 = KubernetesPodOperator(
        name="moe-dv360-to-bigquery",
        task_id="moe-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360()
    )
    kube_cm360 = KubernetesPodOperator(
        name="moe-cm360-to-bigquery",
        task_id="moe-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360()
    )
    kube_ttd = KubernetesPodOperator(
        name="moe-ttd-to-bigquery",
        task_id="moe-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd()
    )
    kube_dash_search = KubernetesPodOperator(
        name="moe-dash-search-to-bigquery",
        task_id="moe-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search()
    )
    kube_ga4_merge = KubernetesPodOperator(
        name="moe-ga4-merge-to-bigquery",
        task_id="moe-ga4_merge_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","ga4_goal_channel"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4_merge()
    )
    kube_dash = KubernetesPodOperator(
        name="moe-dash-to-bigquery",
        task_id="moe-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule = 'all_done',
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash()
        )
    for name,kube_ga4 in ga4_list_task.items():
        kube_ga4 >> kube_ga4_merge
    kube_dash_union = KubernetesPodOperator(
        name="moe-dash-union-to-bigquery",
        task_id="moe-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash()
    )
    set_env_task_tiktok >> kube_tiktok
    set_env_task_facebook >> kube_facebook
    set_env_task_snapchat >> kube_snapchat 
    set_env_task_dv360 >> kube_dv360
    set_env_task_ga4_merge >> kube_ga4_merge
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd 
    set_env_task_google_ads_search >> kube_google_ads_search >>set_env_task_dash_search >> kube_dash_search
    set_env_task_linkedin >> kube_linkedin
    [kube_tiktok,kube_facebook,kube_snapchat,kube_dv360,kube_cm360,kube_ttd,kube_linkedin] >> kube_dash
    kube_dash>>kube_dash_search >> kube_dash_union