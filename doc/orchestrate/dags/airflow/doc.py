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


IMAGE = "australia-southeast1-docker.pkg.dev/doconservation-main/meltano/meltano-doconservation-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    'retry_delay': datetime.timedelta(minutes=10),
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_doconservation_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_developer_main",deserialize_json=True)
    meltano_env_ga4 = Variable.get("meltano_developer_ga4_main",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique, **meltano_env_ga4}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="doconservation-meltano-google-ads",
    schedule_interval="30 13 * * *",
    default_args=default_args
) as google_dag:
    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed'   
   
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
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search'
        return env
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = f"tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed'
        return env
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_task_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_google_ads = PythonOperator(
        task_id="set_env_task_google_ads",
        python_callable=set_env_vars_google_ads_search,
    )
    set_env_task_dash = PythonOperator(
        task_id="set_env_task_dash",
        python_callable=set_env_vars_dash,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_task_dash_search",
        python_callable=set_env_vars_dash_search,
    )
    set_env_task_tiktok = PythonOperator(
        task_id="set_env_task_tiktok",
        python_callable=set_env_vars_tiktok,
    )
    kube_google_ads = KubernetesPodOperator(
        name="doc-google-ads-to-bigquery",
        task_id="doc-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_search(),
        get_logs=True
    )
    kube_tiktok = KubernetesPodOperator(
        name="doc-tiktok-to-bigquery",
        task_id="doc-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run","tap-tiktok","target-bigquery","--full-refresh","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        get_logs=True
    )

    kube_dash = KubernetesPodOperator(
        name="doc-dash-to-bigquery",
        task_id="doc-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule="all_done",
        env_vars=set_env_vars_dash(),
        
        )
    kube_dash_search = KubernetesPodOperator(
        name="doc-dash-search-to-bigquery",
        task_id="doc-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        base_container_name=f"meltano-doc-dash-search",
    )
    kube_ga4 = KubernetesPodOperator(
        name="doc-ga4-to-bigquery",
        task_id="doc-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run","tap-ga4","target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
        get_logs=True
    )

    kube_dash_union = KubernetesPodOperator(
        name="doc-dash-union-to-bigquery",
        task_id="doc-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        )
    kube_tiktok >> kube_google_ads >> kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4
with models.DAG(
    dag_id="doconservation-meltano-extraction-transformation-dbt",
    schedule_interval="0 3 * * *",
    default_args=default_args
) as dag:

    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed'
        return env
    def set_env_vars_linkedin():
        env=get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed'
        return env


    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'doconservation-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table_search'
        return env

    set_env_task_facebook = PythonOperator(
        task_id="set_env_task_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_dv360 = PythonOperator(
        task_id="set_env_task_dv360",
        python_callable=set_env_vars_dv360,
    )

    set_env_task_dash = PythonOperator(
        task_id="set_env_task_dash",
        python_callable=set_env_vars_dash,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_task_dash_search",
        python_callable=set_env_vars_dash_search,
    )

    kube_facebook = KubernetesPodOperator(
        name="doc-facebook-to-bigquery",
        task_id="doc-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        get_logs=True
    )
    kube_linkedin = KubernetesPodOperator(
        name="doc-linkedin-to-bigquery",
        task_id="doc-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery","dbt-bigquery:linkedin_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
        get_logs=True
        #base_container_name=f"meltano-doc-linkedin",
    )
    kube_dv360 = KubernetesPodOperator(
        name="doc-dv360-to-bigquery",
        task_id="doc-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        get_logs=True,
        base_container_name=f"meltano-doc-dv360",
    )

    kube_dash = KubernetesPodOperator(
        name="doc-dash-to-bigquery",
        task_id="doc-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule="all_done",
        env_vars=set_env_vars_dash(),
        base_container_name=f"meltano-doc-dash",
        )
    kube_dash_search = KubernetesPodOperator(
        name="doc-dash-search-to-bigquery",
        task_id="doc-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        base_container_name=f"meltano-doc-dash-search",
    )


    kube_dash_union = KubernetesPodOperator(
        name="doc-dash-union-to-bigquery",
        task_id="doc-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        )
    

 
    set_env_task_facebook >> kube_facebook
    set_env_task_dv360 >> kube_dv360
 

    [kube_facebook,kube_dv360,kube_linkedin] >> kube_dash
    kube_dash >> kube_dash_search
    kube_dash_search >> kube_dash_union 