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


IMAGE = "australia-southeast1-docker.pkg.dev/polestar-main/meltano/meltano-polestar-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': datetime.timedelta(seconds=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}


def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_polestar_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main",deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_analytics_ga4_main",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="polestar-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_ttd():
        env= get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        return env  
    def set_env_vars_facebook():
        env  = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env  
    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    kube_ttd = KubernetesPodOperator(
            name="polestar-ttd-to-bigquery",
            task_id="polestar-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "run","tap-ttd","target-bigquery","dbt-bigquery:ttd_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(),
            get_logs=True,
            execution_timeout=timedelta(minutes=60)
    )
    kube_dash = KubernetesPodOperator(
            name="polestar-dash-to-bigquery",
            task_id="polestar-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
            
        
            )
    kube_linkedin_ads=KubernetesPodOperator(
            name="polestar-linkedin-ads-to-bigquery",
            task_id="polestar-linkedin_ads_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-linkedin-ads","target-bigquery","dbt-bigquery:linkedin_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_linkedin(),
            get_logs=True
    )
    kube_facebook=KubernetesPodOperator(
            name="polestar-facebook-to-bigquery",
            task_id="polestar-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-facebook","target-bigquery","--full-refresh","dbt-bigquery:facebook_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(),
            get_logs=True
            )
    kube_cm360=KubernetesPodOperator(
            name="polestar-cm360-to-bigquery",
            task_id="polestar-cm360_to_bigquery",
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
            name="polestar-dash-union-to-bigquery",
            task_id="polestar-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
    [kube_facebook,kube_cm360,kube_linkedin_ads,kube_ttd] >> kube_dash >> kube_dash_union
    
with models.DAG(
    dag_id="polestar-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as google_dag:

    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env


 

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_ga4():
            env = get_meltano_env()

            env["BQ_DATASET"] = "ga4_raw"
            env["BQ_METHOD"] = "gcs_stage"
            env["DBT_BIGQUERY_METHOD"] = 'oauth'
            env["DBT_BIGQUERY_PROJECT"] = 'polestar-main'
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
            env["TAP_GA4_START_DATE"] = get_ga4_start_date()
            return env
        
    kube_ga4 = KubernetesPodOperator(
            name="polestar-ga4-to-bigquery",
            task_id="polestar-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(),
        ) 

   
   

        
    kube_google_ads_search = KubernetesPodOperator(
            name="polestar-google-ads-search-to-bigquery",
            task_id="polestar-google-ads-search_to_bigquery",
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
            name="polestar-dash-search-to-bigquery",
            task_id="polestar-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
        )
    kube_dash = KubernetesPodOperator(
            name="polestar-dash-to-bigquery",
            task_id="polestar-dash_to_bigquery",
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
            name="polestar-dash-union-to-bigquery",
            task_id="polestar-dash_union_to_bigquery",
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