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


IMAGE = "australia-southeast1-docker.pkg.dev/liquorland-main/meltano/meltano-liquorland-main:prod"


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
today = datetime.datetime.now(local_tz)
# Setting timezone for DAG's start date
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)
start_date_str = start_date.strftime("%Y-%m-%d")
start_date_str = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_liquorland_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    #meltano_env_ga4 = Variable.get("meltano_analytics_ga4_main",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id="liquorland-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        return env
    def set_env_vars_facebook():
        env  = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env  
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env

    kube_cm360 = KubernetesPodOperator(
            name="liquorland-cm360-to-bigquery",
            task_id="liquorland-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:cm360_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360()
    )
    kube_dash = KubernetesPodOperator(
            #name="geely-dash-to-bigquery",
            task_id="liquorland-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash()
        
            )

    kube_facebook=KubernetesPodOperator(
            name="liquorland-facebook-to-bigquery",
            task_id="liquorland-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-facebook","target-bigquery","dbt-bigquery:facebook_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook()
            )
    kube_dash_union = KubernetesPodOperator(
            name="liquorland-dash-union-to-bigquery",
            task_id="liquorland-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
    kube_ttd = KubernetesPodOperator(
            name="liquorland-ttd-to-bigquery",
            task_id="liquorland-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:ttd_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd()
    )
    [kube_facebook,kube_cm360,kube_ttd] >> kube_dash >> kube_dash_union
    
with models.DAG(
    dag_id="liquorland-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as google_dag:

    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env


    def set_env_vars_ga4():
            env = get_meltano_env()

            env["BQ_DATASET"] = "ga4_raw"
            env["BQ_METHOD"] = "gcs_stage"
            env["DBT_BIGQUERY_METHOD"] = 'oauth'
            env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
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
            env["TAP_GA4_START_DATE"] = ga4_start_date_str
            return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'liquorland-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env


   
   

        
    kube_google_ads_search = KubernetesPodOperator(
            name="liquorland-google-ads-search-to-bigquery",
            task_id="liquorland-google-ads-search_to_bigquery",
            namespace="composer-user-workloads",        
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:google_ads_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search()
            )






        
    kube_dash_search = KubernetesPodOperator(
            name="liquorland-dash-search-to-bigquery",
            task_id="liquorland-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
        )
    kube_ga4 = KubernetesPodOperator(
            name="liquorland-ga4-to-bigquery",
            task_id="liquorland-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(),
        ) 
    kube_dash = KubernetesPodOperator(
            name="liquorland-dash-to-bigquery",
            task_id="liquorland-dash_to_bigquery",
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
            name="liquorland-dash-union-to-bigquery",
            task_id="liquorland-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
        

  

    [kube_google_ads_search] >> kube_dash
    kube_dash>>kube_dash_search >> kube_dash_union >> kube_ga4