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


IMAGE = "australia-southeast1-docker.pkg.dev/bepure-main/meltano/meltano-bepure-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

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
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_bepure_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id="bepure-meltano-google-ads",
    schedule_interval=" 0 14 * * *",
    default_args=default_args,
) as google_dag:
    def set_env_vars_ga4(id,label,goal):
        env = get_meltano_env()
        #if goal == 'ecommerce':
        if goal == 'ecommerce':
            env["GA4_REPORTS"] = "./ecommerce_report.json"
            env["GA4_GOAL"] = 'ecmomerce_goal'
        else:
            env["GA4_REPORTS"] = "./report.json"
            env["GA4_GOAL"] = 'goal'   
        env["BQ_DATASET"] = f"ga4_raw__{label}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'       
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_PROPERTY_ID"] = id
        env["TAP_GA4_START_DATE"]  = ga4_start_date_str
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env
    def set_env_vars_tiktok(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"tiktok_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed__{label}'
        env["TAP_TIKTOK_ADVERTISER_ID"] = id
        return env
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    env = get_meltano_env()
    search_list = {}
    ga4_list = {}
    list = {env["GA4_PROPERTY_ID"]:'bepure',env["GA4_EVE_WELLNESS_AU_PROPERTY_ID"]:'eve_wellness_au',
            env['GA4_EVE_WELLNESS_PROPERTY_ID']:'eve_wellness'}
    goal_list = ['ecommerce','goal']
    tiktok_list = {env["TAP_TIKTOK_ADVERTISER_ID__EVE_WELLNESS"]:'eve_wellness'}
    for key,label in tiktok_list.items():
        kube_tiktok = KubernetesPodOperator(
            name=f"{label}-bepure-tiktok-to-bigquery",
            task_id=f"{label}-bepure-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery",f"dbt-bigquery:tiktok_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(key,label),
            #base_container_name=f"meltano-bepure-tiktok",
        )
    for key,label in list.items():
        for goal in goal_list:
            if goal == 'ecommerce':
                arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{label}_{goal}_models"]
            else:
                arguments=["--environment=prod", "invoke",f"dbt-bigquery:ga4_{label}_{goal}_models"]
            kube_ga4 = KubernetesPodOperator(
                name=f"bepure-ga4-{label}-{goal}-to-bigquery",
                task_id=f"bepure-ga4_{label}_{goal}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=arguments,
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_ga4(key,label,goal),
                #base_container_name=f"meltano-bepure-ga4-{label}",
            )
            ga4_list.setdefault(label, []).append(kube_ga4)
            
        kube_google_ads_search = KubernetesPodOperator(
            name=f"{label}-google-ads-search-to-bigquery",
            task_id=f"{label}-google_ads_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",f"dbt-bigquery:google_ads_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            #base_container_name=f"meltano-bepure-google-ads-search",
        )

        search_list.setdefault(label, []).append(kube_google_ads_search)
    list = ['bepure','eve_wellness_au','eve_wellness']
    for label in list:
        kube_dash = KubernetesPodOperator(
            name=f"{label}-bepure-dash-to-bigquery",
            task_id=f"{label}-bepure-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-bepure-dash",
            )
        kube_dash_search = KubernetesPodOperator(
            name=f"{label}-dash-search-to-bigquery",
            task_id=f"{label}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label),
            #base_container_name=f"meltano-bepure-dash-search",
        )
        for task in search_list.get(label, []):
            task >> kube_dash>>kube_dash_search
        kube_dash_union = KubernetesPodOperator(
            name=f"{label}-dash-union-to-bigquery",
            task_id=f"{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-bepure-dash-union",
        )
        for task in ga4_list.get(label, []):
            if list == 'eve_wellness':
                kube_tiktok>>kube_dash >> kube_dash_search >> kube_dash_union>> task
            else:
                kube_dash >> kube_dash_search >> kube_dash_union>> task
        
with models.DAG(
    dag_id="bepure-meltano-extraction-transformation-dbt",
    schedule_interval="0 6 * * *",
    default_args=default_args,
) as dag:
    
    def set_env_vars_facebook(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"]=id
        return env

    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env

    def set_env_vars_dash_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'bepure-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env

    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_dv360 = PythonOperator(
        task_id="set_env_dv360",
        python_callable=set_env_vars_dv360,
    )


    
    env = get_meltano_env()
    dv_list = {}
    kube_cm360 = KubernetesPodOperator(
        name="bepure-cm360-transformation",
        task_id = "bepure-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        env_vars = set_env_vars_cm360(),
    )

   
    facebook_list = {env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID__BEPURE"]:'bepure',
                     env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID__EVE_WELLNESS"]:'eve_wellness',
                     env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID__EVE_WELLNESS_AU"]:'eve_wellness_au'}
    for key,label in facebook_list.items():
        kube_facebook = KubernetesPodOperator(
            name=f"{label}-bepure-facebook-to-bigquery",
            task_id=f"{label}-bepure-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(key,label),
            #base_container_name=f"meltano-bepure-facebook",
        )
        dv_list.setdefault(label, []).append(kube_facebook)

    kube_dv360 = KubernetesPodOperator(
        name="bepure-dv360-to-bigquery",
        task_id="bepure-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        #base_container_name=f"meltano-bepure-dv360",
    )
    list = ['bepure','eve_wellness_au','eve_wellness']
  


    
    list = ['bepure','eve_wellness_au','eve_wellness']
    for label in list:
        kube_dash = KubernetesPodOperator(
            name=f"{label}-bepure-dash-to-bigquery",
            task_id=f"{label}-bepure-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-bepure-dash",
            )
        for task in dv_list.get(label, []):
            if label != 'bepure':
                task >> kube_dash
            else:
                 [kube_dv360,kube_cm360,task] >> kube_dash

        kube_dash_search = KubernetesPodOperator(
            name=f"{label}-dash-search-to-bigquery",
            task_id=f"{label}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label),
            #base_container_name=f"meltano-bepure-dash-search",
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"{label}-dash-union-to-bigquery",
            task_id=f"{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-bepure-dash-union",
        )
        [kube_dash,kube_dash_search] >> kube_dash_union
    
    set_env_task_dv360 >> kube_dv360

    set_env_task_cm360 >> kube_cm360
   