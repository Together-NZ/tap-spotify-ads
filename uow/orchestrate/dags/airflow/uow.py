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
from comparison_package import ComparisonTrigger
import time
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import secretmanager
from google.cloud import storage 
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
import json


IMAGE = "australia-southeast1-docker.pkg.dev/uowaikato-main/meltano/meltano-uowaikato-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
ga4_start_date = datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
comparison_end_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 12, 15, tzinfo=local_tz),
    'retry_delay': timedelta(minutes=30),
    'email': ["tayaza@wearetogether.co.nz","keivn@wearetogether.co.nz"],
    'email_on_failure': True
}


def load_secrets_from_secret_manager(secret_prefix: str, project_id: str):
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_id}"
    
    secrets = {}
    for secret in client.list_secrets(request={"parent": parent}):
        name = secret.name.split("/")[-1]
        if not name.startswith(secret_prefix):
            continue

        # Get latest version
        version_path = f"{parent}/secrets/{name}/versions/latest"
        response = client.access_secret_version(name=version_path)
        value = response.payload.data.decode("UTF-8")

        # Strip prefix for clean env var names
        env_name = name.replace(f"{secret_prefix}_", "")
        secrets[env_name] = value
    return secrets

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env = Variable.get("meltano_uowaikato_main", deserialize_json=True)
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=4)
    start_date_str = yesterday.strftime("%Y-%m-%d")
    #meltano_env["TAP_TIKTOK_START_DATE"]=start_date_str
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")

def get_meta_start_date():
    return (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=30)
    ).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_linkedin_start_date():
    return datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
with models.DAG(
    dag_id="uowaikato-meltano-google-ads",
    schedule_interval="10 14 * * *",
    default_args=default_args,
) as google_dag:
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
        return env
    def set_env_vars_google_ads():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'  
        env["TAP_GA4_START_DATE"] = get_ga4_start_date() 
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_tiktok = PythonOperator(
        task_id="set_env_tiktok",
        python_callable=set_env_vars_tiktok,
    )
    set_env_task_google_ads = PythonOperator(
        task_id="set_env_google_ads",
        python_callable=set_env_vars_google_ads,
    )
    kube_ga4 = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-ga4-to-bigquery",
        task_id="uow-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
        get_logs=True
    )
    kube_tiktok = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-tiktok-to-bigquery",
        task_id="uow-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","--full-refresh","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        
        get_logs=True
    )
    kube_google_ads = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-google-ads-to-bigquery",
        task_id="uow-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "google_ads"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
        base_container_name=f"meltano-uow-google-ads",
    )
    kube_dash = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-dash-to-bigquery",
        task_id="uow-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule="all_done",
        env_vars=set_env_vars_dash(),
        
        )
    kube_dash_union = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-dash-union-to-bigquery",
        task_id="uow-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        )
    env = get_meltano_env()
    comparison_trigger_tiktok = ComparisonTrigger(
        project_name="uowaikato-main",
        destination_table="tiktok_transformed",
        table_name="tiktok",
        source_name="tiktok",
        start_date=comparison_start_date,
        end_date=(datetime.datetime.now(local_tz) - timedelta(days=1)).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_uowaikato_main",
        project_id=env["PROJECT_ID"])
    def tiktok_comparison_check(**context):
        result = comparison_trigger_tiktok.compare_data()
        if not result:
            raise ValueError("Tiktok data accuracy check failed — BQ data does not match source API.")
        return result
    task_tiktok_comparison = PythonOperator(
        task_id="task_tiktok_comparison",
        python_callable=tiktok_comparison_check,
        retries=0,
        trigger_rule="all_done",
    )
    set_env_task_ga4 >> kube_ga4 >> task_tiktok_comparison
    set_env_task_google_ads >> kube_google_ads
    [kube_tiktok,kube_google_ads] >> kube_dash >> kube_dash_union >> kube_ga4
with models.DAG(
    dag_id="uowaikato-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"]=get_meta_start_date()
        return env
    def set_env_vars_snapchat():
        env = get_meltano_env()
        env["BQ_DATASET"] = "snapchat_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'snapchat_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"] =get_ttd_start_date()
        return env

    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        env['TAP_LINKEDIN_ADS_START_DATE'] = get_linkedin_start_date()
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'uowaikato-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env


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
        email_on_failure=True,
        name="uow-linkedin-to-bigquery",
        task_id="uow-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments = [
             "--environment=prod", "run","tap-linkedin-ads","target-bigquery",
            "dbt-bigquery:linkedin_models"
        ],
        env_vars=set_env_vars_linkedin(),
        get_logs=True
    )
 
    
    kube_facebook = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-facebook-to-bigquery",
        task_id="uow-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",
                   "--full-refresh",
                   "dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        get_logs=True
    )
    kube_snapchat = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-snapchat-to-bigquery",
        task_id="uow-snapchat_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery","--full-refresh","dbt-bigquery:snapchat_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_snapchat(),
        get_logs=True
    )
    kube_dv360 = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-dv360-to-bigquery",
        task_id="uow-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        base_container_name=f"meltano-uow-dv360",
    )
    kube_cm360 = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-cm360-to-bigquery",
        task_id="uow-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        #base_container_name=f"meltano-uow-cm360",
    )
    kube_ttd = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-ttd-to-bigquery",
        task_id="uow-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        get_logs=True,
        execution_timeout=timedelta(minutes=80)
    )

    kube_dash = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-dash-to-bigquery",
        task_id="uow-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule="all_done",
        env_vars=set_env_vars_dash(),
        
        )
    kube_dash_union = KubernetesPodOperator(
        email_on_failure=True,
        name="uow-dash-union-to-bigquery",
        task_id="uow-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        )
    env = get_meltano_env()
    comparison_trigger_facebook = ComparisonTrigger(
        project_name="uowaikato-main",
        destination_table="facebook_transformed",
        table_name="facebook",
        source_name="meta",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_uowaikato_main",
        project_id=env["PROJECT_ID"]
        )

    comparison_trigger_linkedin = ComparisonTrigger(
        project_name="uowaikato-main",
        destination_table="linkedin_transformed",
        table_name="linkedin",
        source_name="linkedin",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_uowaikato_main",
        project_id=env["PROJECT_ID"]
    )

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

    set_env_task_facebook >> kube_facebook >> task_facebook_comparison
    set_env_task_snapchat >> kube_snapchat 
    set_env_task_dv360 >> kube_dv360
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd 
    set_env_task_linkedin >> kube_linkedin >> task_linkedin_comparison
    [kube_facebook,kube_snapchat,kube_dv360,kube_cm360,kube_ttd,kube_linkedin] >> kube_dash
    kube_dash >> kube_dash_union