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


IMAGE = "australia-southeast1-docker.pkg.dev/volvo-main/meltano/meltano-volvo-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_volvo_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = meltano_env_unique
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

with models.DAG(
    dag_id="volvo-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_hivestack(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"hivestack_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'hivestack_transformed__{label}'
        return env
    def set_env_vars_facebook(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[f"{label}_TAP_FACEBOOK_ACCOUNT_ID"]
        return env
    def set_env_vars_ga4(value):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw__{value}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
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
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_PROPERTY_ID"] = env[f'{value}_TAP_GA4_PROPERTY_ID']
        return env
    def set_env_vars_linkedin(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"linkedin_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'linkedin_transformed__{label}'
        return env
    def set_env_vars_dv360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
        return env
    def set_env_vars_ttd(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ttd_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
        return env
    def set_env_vars_google_ads(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_display_video"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_dv_transformed__{label}'
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env

    def set_env_vars_dash_search(label):
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env

    brands =['volvo']
    for brand in brands:
        kube_linkedin=KubernetesPodOperator(
            name=f"{brand}-linkedin-to-bigquery",
            task_id=f"{brand}-linkedin_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery",f"dbt-bigquery:linkedin_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_linkedin(brand),
            get_logs=True
        )
        kube_hivestack=KubernetesPodOperator(
            name=f"{brand}-hivestack-to-bigquery",
            task_id=f"{brand}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",f"dbt-bigquery:hivestack_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(brand),
            get_logs=True
        )
        kube_ga4 = KubernetesPodOperator(
            name=f"{brand}-ga4-to-bigquery",
            task_id=f"{brand}-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(brand),
        )        

        kube_dv360 = KubernetesPodOperator(
            name=f"{brand}-dv360-to-bigquery",
            task_id=f"{brand}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",f"dbt-bigquery:dv360_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(brand),
            
        
        )
        kube_ttd = KubernetesPodOperator(
            name=f"{brand}-ttd-to-bigquery",
            task_id=f"{brand}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery",f"dbt-bigquery:ttd_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(brand),
            
        
        )
    brands=['volvo']
    for brand in brands:
        
        kube_google_ads_search = KubernetesPodOperator(
            name=f"{brand}-google-ads-search-to-bigquery",
            task_id=f"{brand}-google-ads-search_to_bigquery",
            namespace="composer-user-workloads",        
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"google_ads_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True
            )

        kube_facebook = KubernetesPodOperator(
            name=f"{brand}-facebook-to-bigquery",
            task_id=f"{brand}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(brand),
            get_logs=True
        
            
        )
        kube_cm360 = KubernetesPodOperator(
            name=f"{brand}-cm360-to-bigquery",
            task_id=f"{brand}-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360(brand),

        
        )


        kube_google_ads = KubernetesPodOperator(
            name=f"{brand}-google-dv-to-bigquery",
            task_id=f"{brand}-google_ads_dv_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_dv__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads(brand),
            
        
        )
        kube_dash_search = KubernetesPodOperator(
            name=f"{brand}-dash-search-to-bigquery",
            task_id=f"{brand}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
        )
        kube_dash = KubernetesPodOperator(
            name=f"{brand}-dash-to-bigquery",
            task_id=f"{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            
        
            )
        kube_google_ads_demand = KubernetesPodOperator(
            name = f"{brand}-google-ads-demand-to-bigquery",
            task_id=f"{brand}-google-ads-demand_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"google_ads_demand__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"{brand}-dash-union-to-bigquery",
            task_id=f"{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )
        

        [kube_google_ads_search]>>kube_dash_search

        [kube_facebook,kube_dv360,kube_cm360,kube_linkedin,kube_ttd,kube_google_ads,kube_hivestack,kube_google_ads_demand] >> kube_dash
        kube_dash>>kube_dash_search >> kube_dash_union >> kube_ga4