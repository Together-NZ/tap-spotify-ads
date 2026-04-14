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
from comparison_package import ComparisonTrigger
import json
import time
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import secretmanager
from google.cloud import storage 
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
import json


IMAGE = "australia-southeast1-docker.pkg.dev/zeekr-main/meltano/meltano-zeekr-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
    "email_on_failure": True,
    'retry_delay': timedelta(minutes=30)
}
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_zeekr_main", deserialize_json=True)
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

def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="zeekr-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    
    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_ttd():
        env=get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env
    kube_ttd=KubernetesPodOperator(
            name="zeekr-ttd-to-bigquery",
            task_id="zeekr-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-ttd","target-bigquery","dbt-bigquery:ttd_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(),
            execution_timeout=timedelta(minutes=60)
    )
    kube_facebook = KubernetesPodOperator(
            name="zeekr-facebook-to-bigquery",
            task_id="zeekr-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-facebook","target-bigquery","dbt-bigquery:facebook_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(),
    )
    kube_linkedin=KubernetesPodOperator(
            name="zeekr-linkedin-to-bigquery",
            task_id="zeekr-linkedin_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-linkedin-ads","target-bigquery","dbt-bigquery:linkedin_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_linkedin(),
    )
    kube_dash_search = KubernetesPodOperator(
            name="zeekr-dash-search-to-bigquery",
            task_id="zeekr-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
        )
    kube_dash = KubernetesPodOperator(
            name="zeekr-dash-to-bigquery",
            task_id="zeekr-dash_to_bigquery",
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
            name="zeekr-dash-union-to-bigquery",
            task_id="zeekr-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
    env=get_meltano_env()
    comparison_trigger_facebook = ComparisonTrigger(
        project_name="zeekr-main",
        destination_table="facebook_transformed",
        table_name="facebook",
        source_name="meta",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_zeekr_main",
        project_id=env["PROJECT_ID"]
        )
    comparison_trigger_linkedin = ComparisonTrigger(
        project_name="zeekr-main",
        destination_table="linkedin_transformed",
        table_name="linkedin",
        source_name="linkedin",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_zeekr_main",
        project_id=env["PROJECT_ID"]
    )
    comparison_trigger_linkedin.compare_data()
    def linkedin_comparison_check(**context):
        result = comparison_trigger_linkedin.compare_data()
        if not result:
            raise ValueError("Linkedin data accuracy check failed — BQ data does not match source API.")
        return result
    task_linkedin_comparison = PythonOperator(
        task_id="task_linkedin_comparison",
        python_callable=linkedin_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    def facebook_comparison_check(**context):
        result = comparison_trigger_facebook.compare_data()
        if not result:
            raise ValueError("Facebook data accuracy check failed — BQ data does not match source API.")
        return result

    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=facebook_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    kube_facebook >> task_facebook_comparison
    kube_linkedin >> task_linkedin_comparison
    [kube_facebook,kube_linkedin] >> kube_ttd,kube_dash >> kube_dash_search >> kube_dash_union
with models.DAG(
    dag_id="zeekr-meltano-google_ads",
    schedule_interval="30 13 * * *",
    default_args=default_args
) as dag:
    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env

  
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
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


 

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env

    def set_env_vars_dash_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'zeekr-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env


    kube_ga4 = KubernetesPodOperator(
            #name="zeekr-ga4-to-bigquery",
            task_id="zeekr-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(),
        )        


   
  
        
    kube_google_ads= KubernetesPodOperator(
            name="zeekr-google-ads-search-to-bigquery",
            task_id="zeekr-google-ads-search_to_bigquery",
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
            name="zeekr-dash-search-to-bigquery",
            task_id="zeekr-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
        )
    kube_dash = KubernetesPodOperator(
            name="zeekr-dash-to-bigquery",
            task_id="zeekr-dash_to_bigquery",
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
            name="zeekr-dash-union-to-bigquery",
            task_id="zeekr-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
        )
        

 
    kube_google_ads>>kube_dash>>kube_dash_search >> kube_dash_union >> kube_ga4