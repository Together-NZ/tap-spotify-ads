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


IMAGE = "australia-southeast1-docker.pkg.dev/mpi-main/meltano/meltano-mpi-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': datetime.timedelta(seconds=30),
    "start_date": datetime.datetime(2026, 1, 22, tzinfo=local_tz)
}


def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_mpi_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main",deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="mpi-meltano-extraction-transformation-dbt",
    schedule_interval="0 3 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env

    def set_env_vars_facebook():
        env  = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env  
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    kube_dash = KubernetesPodOperator(
            name="mpi-dash-to-bigquery",
            task_id="mpi-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
            
        
            )

    kube_facebook=KubernetesPodOperator(
            name="mpi-facebook-to-bigquery",
            task_id="mpi-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-facebook","target-bigquery","dbt-bigquery:facebook_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(),
            get_logs=True
            )
    kube_cm360=KubernetesPodOperator(
            name="mpi-cm360-to-bigquery",
            task_id="mpi-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:cm360_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360(),
            get_logs=True
    )
    kube_dash_union = KubernetesPodOperator(
            name="mpi-dash-union-to-bigquery",
            task_id="mpi-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
    kube_dv360=KubernetesPodOperator(
            name="mpi-dv360-to-bigquery",
            task_id="mpi-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-dv360","target-bigquery","dbt-bigquery:dv360_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(),
            get_logs=True
    )
    [kube_facebook,kube_cm360,kube_dv360] >> kube_dash >> kube_dash_union
    
with models.DAG(
    dag_id="mpi-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as google_dag:

    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env


 

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_AUTH_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
  
    def set_env_vars_ga4(goal):
        env = get_meltano_env()
        #if goal == 'ecommerce':
        if goal == 'sessions':
            env["TAP_GA4_REPORTS"] = "./report_sessions.json"
            env["GA4_GOAL"] = 'session_goal'
        else:
            env["TAP_GA4_REPORTS"] = "./report.json"
            env["GA4_GOAL"] = 'goal'   
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'mpi-main'
        env["DBT_BIGQUERY_AUTH_METHOD"]='oauth'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'       
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_START_DATE"]  = get_ga4_start_date()
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env
 
   
   

        
    kube_google_ads_search = KubernetesPodOperator(
            name="mpi-google-ads-search-to-bigquery",
            task_id="mpi-google-ads-search_to_bigquery",
            namespace="composer-user-workloads",        
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:google_ads_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(),
            get_logs=True
            )






        
    kube_dash_search = KubernetesPodOperator(
            name="mpi-dash-search-to-bigquery",
            task_id="mpi-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
        )
    kube_dash = KubernetesPodOperator(
            name="mpi-dash-to-bigquery",
            task_id="mpi-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
            
        
            )

    kube_dash_union = KubernetesPodOperator(
            name="mpi-dash-union-to-bigquery",
            task_id="mpi-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
        

  
    goal_list = ['sessions','goal']
    for label in goal_list:
        
        kube_ga4 = KubernetesPodOperator(
                name=f"mpi-ga4-to-bigquery-{label}",
                task_id=f"mpi-ga4_to_bigquery_{label}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{label}_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_ga4(goal=label),
            ) 
        kube_dash_union >> kube_ga4
    kube_ga4_final=KubernetesPodOperator(
            name="mpi-ga4-to-bigquery-final",
            task_id="mpi-ga4_to_bigquery_final",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","ga4_goal_channel"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(goal=label),
            get_logs=True
        )
    kube_ga4 >> kube_ga4_final

    [kube_google_ads_search] >> kube_dash
    kube_dash>>kube_dash_search >> kube_dash_union 