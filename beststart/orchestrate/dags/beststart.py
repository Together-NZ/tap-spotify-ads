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


IMAGE = "australia-southeast1-docker.pkg.dev/best-start-main/meltano/meltano-best-start-main:prod"


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
ga4_start_date_str = (datetime.datetime.now(local_tz)-datetime.timedelta(days=30)).strftime("%Y-%m-%d")
end_date_str = datetime.datetime.now(local_tz).strftime("%Y-%m-%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_beststart_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id= "beststart-meltano-google-ads",
    schedule_interval="00 14 * * *",
    default_args=default_args,
) as google_dag:
    def set_env_vars_google_ads_search(label):
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        return env
    def set_env_vars_dash_search(label):
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_dash_search_union():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_google_ads():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_dv_transformed'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'       
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        env["TAP_GA4_END_DATE"] = end_date_str
        env["TAP_GA4_START_DATE"] = ga4_start_date_str
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table'
        return env
    kube_dash_search_union = KubernetesPodOperator(
        name="beststart-dash-search-to-bigquery",
        task_id="beststart-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search_union(),
    )
    kube_dash_union = KubernetesPodOperator(
        name="beststart-dash-union-to-bigquery",
        task_id="beststart-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )
    kube_google_ads_list = {}
    list = ['beststart','hr_career']
    for label in list:
        kube_google_ads_search = KubernetesPodOperator(
            name=f"beststart-google-ads-search-to-bigquery-{label}",
            task_id=f"beststart-google_ads_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke",f"dbt-bigquery:google_ads_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(label),
            
        )
        kube_google_ads_list.setdefault(label,[]).append(kube_google_ads_search)
    kube_ga4 = KubernetesPodOperator(
        name="beststart-ga4-to-bigquery",
        task_id="beststart-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
        
    )

    kube_google_ads = KubernetesPodOperator(
        name="beststart-google-ads-to-bigquery",
        task_id="beststart-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "google_ads_dv"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
       )
    kube_dash = KubernetesPodOperator(
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
        namespace="composer-user-workloads",
        trigger_rule = 'all_done',
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash()
        )
    for label in list:
        kube_dash_search = KubernetesPodOperator(
            name=f"beststart-dash-search-to-bigquery-{label}",
            task_id=f"beststart-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label)
        )
        for upstream in kube_google_ads_list.get(label):
            upstream >> kube_dash >> kube_dash_search
            kube_dash_search >> kube_dash_search_union
    kube_dash_search_union >> kube_dash_union >> kube_ga4 
        
    
with models.DAG(
    dag_id="beststart-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_dash_search(label):
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
        return env
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
        return env
    def set_env_vars_dash_search_union():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env


    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'best-start-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table'
        return env

    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_tiktok = PythonOperator(
        task_id="set_env_tiktok",
        python_callable=set_env_vars_tiktok,
    )

    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_dv360 = PythonOperator(
        task_id="set_env_dv360",
        python_callable=set_env_vars_dv360,
    )

    
    kube_tiktok = KubernetesPodOperator(
        name="beststart-tiktok-to-bigquery",
        task_id="beststart-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run","tap-tiktok","target-bigquery","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
    )
    kube_dash_search_union = KubernetesPodOperator(
        name="beststart-dash-search-to-bigquery",
        task_id="beststart-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search_union(),
    )
    kube_dash_union = KubernetesPodOperator(
        name="beststart-dash-union-to-bigquery",
        task_id="beststart-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )

    kube_dv360 = KubernetesPodOperator(
        name="beststart-dv360-to-bigquery",
        task_id="beststart-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
       
    )

    kube_cm360 = KubernetesPodOperator(
        name="beststart-cm360-transformation",
        task_id = "beststart-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        env_vars = set_env_vars_cm360(),
        
    )
    
    kube_facebook = KubernetesPodOperator(
        name="beststart-facebook-to-bigquery",
        task_id="beststart-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        
    )




    kube_dash = KubernetesPodOperator(
        name="beststart-dash-to-bigquery",
        task_id="beststart-dash_to_bigquery",
        namespace="composer-user-workloads",
        trigger_rule = 'all_done',
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash()
        )
    for label in list:
        kube_dash_search = KubernetesPodOperator(
            name=f"beststart-dash-search-to-bigquery-{label}",
            task_id=f"beststart-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(label)
        )
        kube_dash_search >> kube_dash_search_union

    set_env_task_facebook >> kube_facebook
    set_env_task_tiktok >> kube_tiktok
    set_env_task_cm360 >> kube_cm360
    set_env_task_dv360 >> kube_dv360
    [kube_facebook,kube_cm360,kube_dv360,kube_tiktok] >> kube_dash
    kube_dash >> kube_dash_search_union >> kube_dash_union