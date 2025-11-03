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


IMAGE = "australia-southeast1-docker.pkg.dev/contact-energy-main/meltano/meltano-contact-main:prod"


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
today_date_str = datetime.datetime.now(local_tz).strftime("%Y-%m-%d")
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y%m%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_contact_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id = 'contact-energy-google_ads_search',
    schedule_interval = '0 14 * * *',
    default_args = default_args
) as dag_google_ads:
    def set_env_vars_ga4(label,type):
            env = get_meltano_env()
            if type == 'goal':
                env["GA4_REPORTS"] = './report.json'
                env["GA4_GOAL"] = 'goal'
            else:
                env["GA4_REPORTS"] = './ecommerce_report.json'
                env["GA4_GOAL"] = 'ecommerce_goal'
            env["BQ_DATASET"] = f"ga4_raw"
            env["BQ_METHOD"] = "gcs_stage"
            env["DBT_BIGQUERY_METHOD"] = 'oauth'
            env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
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
            env["TAP_GA4_START_DATE"] = ga4_start_date_str
            return env
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash(label):
        
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    search_queue = {}
    ga4_types = ['goal','ecommerce']
    search_list = ["mobile","broadband","energy"]
    for brand in search_list:
        kube_google_ads_search = KubernetesPodOperator(
            name=f"contact-{brand}-google-ads-search-to-bigquery",
            task_id=f"contact-{brand}_google_ads_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"google_ads_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        #search_queue.setdefault(brand, []).append(kube_google_ads_search)
        kube_dash_search = KubernetesPodOperator(
            name=f"contact-{brand}-dash-search-to-bigquery",
            task_id=f"contact-{brand}_dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"dash_table_search__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        kube_google_ads_search >> kube_dash_search
        kube_dash_union = KubernetesPodOperator(
            name=f"contact-{brand}-dash-union-to-bigquery",
            task_id=f"contact-{brand}_dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"dash_union__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        kube_dash_search >> kube_dash_union
    kube_ga4_mobile_goal = KubernetesPodOperator(
                    name=f"contact-ga4-to-bigquery-mobile-goal",
                    task_id=f"contact_ga4_to_bigquery_mobile_goal",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                            
                    arguments=[
                        "--environment=prod",
                        "run",
                        "tap-ga4",
                        "target-bigquery",
                        f"dbt-bigquery:ga4_{brand}_goal_models"
                    ],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_ga4(brand,'goal'),
                )
    kube_dash_union >> kube_ga4_mobile_goal
    kube_ga4_mobile_ecommerce = KubernetesPodOperator(
                    name=f"contact-ga4-to-bigquery-mobile-ecommerce",
                    task_id=f"contact_ga4_to_bigquery_mobile_ecommerce",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                            
                    arguments=[
                        "--environment=prod",
                        "run",
                        "tap-ga4",
                        "target-bigquery",
                        f"dbt-bigquery:ga4_{brand}_ecommerce_models"
                    ],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_ga4(brand,'ecommerce'),
                )
    kube_ga4_mobile_goal >> kube_ga4_mobile_ecommerce
    
    ga4_brands=["broadband","energy","all"]
    for brand in ga4_brands:
        for type in ga4_types:
            kube_ga4 = KubernetesPodOperator(
                    name=f"contact-ga4-to-bigquery-{brand}-{type}",
                    task_id=f"contact_ga4_to_bigquery_{brand}_{type}",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    
                    arguments=[
                        "--environment=prod",
                        "invoke",
                        f"dbt-bigquery:ga4_{brand}_{type}_models"
                    ],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_ga4(brand,type),
                )
            kube_ga4_mobile_ecommerce>>kube_ga4
            
with models.DAG(
    dag_id="contact-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:
    
    def set_env_vars_facebook(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        return env

 
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_search(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
        return env
    def set_env_vars_tiktok(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed__{label}'
        return env
    def set_env_vars_ttd(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
        return env
    def set_env_vars_dash(label):
        
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_dv360(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'contact-energy-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
        return env
    brands = ['mobile','broadband','energy']
    queue = {}

    
    for brand in brands:
        kube_cm360 = KubernetesPodOperator(
            name=f"contact-{brand}-cm360-to-bigquery",
            task_id=f"contact-{brand}_cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",f"dbt-bigquery:cm360_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        queue.setdefault(brand, []).append(kube_cm360)
        kube_tiktok = KubernetesPodOperator(
            name=f"contact-{brand}-tiktok-to-bigquery",
            task_id=f"contact-{brand}_tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery",f"dbt-bigquery:tiktok_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )                
        queue.setdefault(brand, []).append(kube_tiktok)

        kube_google_demand = KubernetesPodOperator(
            name=f"contact-{brand}-google-demand-to-bigquery",
            task_id=f"contact-{brand}_google_demand_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"google_ads_demand__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        queue.setdefault(brand, []).append(kube_google_demand)
        kube_facebook = KubernetesPodOperator(
            #name=f"contact-{brand}-facebook-to-bigquery",
            task_id=f"contact-{brand}_facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        queue.setdefault(brand, []).append(kube_facebook)

        kube_dv360 = KubernetesPodOperator(
            name=f"contact-{brand}-dv360-to-bigquery",
            task_id=f"contact-{brand}_dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-dv360","target-bigquery", f"dbt-bigquery:dv360_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        queue.setdefault(brand, []).append(kube_dv360)
        kube_ttd = KubernetesPodOperator(
            name=f"contact-{brand}-ttd-to-bigquery",
            task_id=f"contact-{brand}_ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-ttd","target-bigquery", f"dbt-bigquery:ttd_{brand}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        queue.setdefault(brand, []).append(kube_ttd)


    for brand in brands:
        kube_dash = KubernetesPodOperator(
            name=f"contact-{brand}-dash-to-bigquery",
            task_id=f"contact-{brand}_dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"dash_table__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
            trigger_rule = 'all_done',
            is_delete_operator_pod=True,
        )
        kube_dash_search = KubernetesPodOperator(
            name=f"contact-{brand}-dash-search-to-bigquery",
            task_id=f"contact-{brand}_dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"dash_table_search__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"contact-{brand}-dash-union-to-bigquery",
            task_id=f"contact-{brand}_dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select",f"dash_union__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
            is_delete_operator_pod=True,
        )

        kube_dash >> kube_dash_search
        for upstream in queue.get(brand):
            upstream >> kube_dash
        kube_dash_search >> kube_dash_union
 
