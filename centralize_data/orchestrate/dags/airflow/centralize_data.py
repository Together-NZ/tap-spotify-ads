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


IMAGE = "australia-southeast1-docker.pkg.dev/together-internal/meltano/meltano-together-internal:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2026, 3, 5, tzinfo=local_tz)
}
# Setting timezone for DAG's start date
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)
start_date_str = start_date.strftime("%Y-%m-%d")
start_date_str = yesterday.strftime("%Y-%m-%d")

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
    meltano_env = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy

with models.DAG(
    dag_id="centralized-meltano-extraction-transformation-dbt",
    schedule_interval="0 0 * * *",
    default_args=default_args,
) as dag:
    def set_env_vars_search():
        env=get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'together-internal'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_campaign_location'
        return env
    def set_env_vars_mapping():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'together-internal'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_campaign_mapping'
        return env
    def set_env_vars_cm360_contact():
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw__contact"
        env["BQ_METHOD"] = "batch_job"
        env['TAP_CM360_PROFILE_ID']='10275036'
      
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env['TAP_CM360_PROFILE_ID']='9840205'

        return env
    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'together-internal'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_spotifyads():
        env = get_meltano_env()
        env["BQ_DATASET"] = "spotify_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'together-internal'
        env["DBT_BIGQUERY_DATASET"] = 'spotify_transformed'
        return env
    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_mapping = PythonOperator(
        task_id='set_env_mapping',
        python_callable=set_env_vars_mapping,
    )
    set_env_task_cm360_contact = PythonOperator(
        task_id = "set_env_cm360_contact",
        python_callable=set_env_vars_cm360_contact
    )
    set_env_task_linkedin = PythonOperator(
        task_id = "set_env_linkedin",
        python_callable=set_env_vars_linkedin
    )
    set_env_task_spotifyads = PythonOperator(
        task_id = "set_env_spotifyads",
        python_callable=set_env_vars_spotifyads
    )
    kube_google_mapping = KubernetesPodOperator(
        name='google_ads_mapping',
        task_id='google_ads_mapping',
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","campaign_name_mapping"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_mapping(),
    )
    kube_spotifyads = KubernetesPodOperator(
        name="spotifyads-to-bigquery",
        task_id="spotifyads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run","tap-spotifyads","target-bigquery", "dbt-bigquery","invoke","run","--select","spotify_ads"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_spotifyads(),
    )

    kube_google_ads_search_location=KubernetesPodOperator(
        name="google-ads-search-location",
        task_id="google_ads_search_location",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","google_ads_search_geo_target"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_search(),
    )
    kube_cm360 = KubernetesPodOperator(
        name="cm360-to-bigquery",
        task_id="cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-cm360", "target-bigquery","--full-refresh"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
    )
    kube_cm360_contact = KubernetesPodOperator(
        name="cm360_contact-to-bigquery",
        task_id="cm360_contact_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-cm360", "target-bigquery","--full-refresh"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360_contact(),
    )

    kube_linkedin = KubernetesPodOperator(
        name="linkedin-to-bigquery",
        task_id="linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery","dbt-bigquery:linkedin_models",
                   "run_python:naming"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
    )
    set_env_task_mapping >> kube_google_mapping
    set_env_task_cm360 >> kube_cm360 
    set_env_task_spotifyads >> kube_spotifyads
    set_env_task_cm360_contact >> kube_cm360_contact
    set_env_task_linkedin >> kube_linkedin 
