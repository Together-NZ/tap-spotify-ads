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


IMAGE = "australia-southeast1-docker.pkg.dev/inghams-main/meltano/meltano-inghams-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=13)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}

kube_downstream_dependencies = []


def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_inghams_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id="inghams-meltano-google-ads",
    schedule_interval="10 14 * * *",
    default_args=default_args,
) as google_dag:
    def set_env_vars_tiktok(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"tiktok_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'tiktok_transformed__{label}'
        env["TAP_TIKTOK_ADVERTISER_ID"] = id
        return env
    def set_env_vars_dash(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_ga4(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw__{label}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
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
        env["TAP_GA4_PROPERTY_ID"] = id
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        return env  
    env=get_meltano_env()
    ga4_list = {env["TAP_GA4_PROPERTY_ID_WAITOA"]:'waitoa',env["TAP_GA4_PROPERTY_ID_INGHAMS"]:'inghams'}
    for key,label in ga4_list.items():
        kube_ga4 = KubernetesPodOperator(
            name=f"{label}-ga4-to-bigquery",
            task_id=f"{label}-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery",f"dbt-bigquery:ga4_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(key,label
            )
        )
    tiktok_list = {env["TIKTOK_WAITOA_ADVERTISER_ID"]:'waitoa'}
    for key,label in tiktok_list.items():
        kube_tiktok = KubernetesPodOperator(
            name=f"{label}-tiktok-to-bigquery",
            task_id=f"{label}-tiktok_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","--full-refresh",f"dbt-bigquery:tiktok_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_tiktok(key,label
)
        )
        kube_dash = KubernetesPodOperator(
            name=f"{label}-dash-to-bigquery",
            task_id=f"{label}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
            )
        kube_dash_union=KubernetesPodOperator(
            name=f"{label}-dash-union-to-bigquery",
            task_id=f"{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
        )
        kube_tiktok >> kube_dash >> kube_dash_union
        

with models.DAG(
    dag_id="inghams-meltano-extraction-transformation-dbt",
    schedule_interval="0 6 * * *",
    default_args=default_args,
) as dag:
    
    def set_env_vars_hivestack(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"hivestack_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'hivestack_transformed__{label}'
        env["TAP_HIVESTACK_REPORT_ID"]= id
        return env      
    def set_env_vars_facebook(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"]=id
        return env
    def set_env_vars_cm360(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
        return env
    def set_env_vars_dv360(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
        env["TAP_DV360_ADVERTISER_ID"]=id
        return env

    def set_env_vars_ttd(id,label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ttd_raw__{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'inghams-main'
        env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
        env["TAP_TTD_ADVERTISER_ID"] = id
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env




    env = get_meltano_env()

    hive_list = {env["HIVESTACK_WAITOA_ID"]:'waitoa',env["HIVESTACK_INGHAMS_ID"]:'inghams'}
    for key,label in hive_list.items():

        kube_hivestack = KubernetesPodOperator(
            name=f"{label}-hivestack-to-bigquery",
            task_id=f"{label}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",f"dbt-bigquery:hivestack_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(key,label)
        )
        kube_downstream_dependencies.append(kube_hivestack)

    facebook_list = {env["FACEBOOK_WAITOA_ID"]:'waitoa',env["FACEBOOK_INGHAMS_ID"]:'inghams'}
    for key,label in facebook_list.items():
        kube_facebook = KubernetesPodOperator(
            name=f"inghams-{label}-facebook-to-bigquery",
            task_id=f"inghams-{label}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{label}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(key,label),
            #base_container_name=f"meltano-{label}-facebook",
            get_logs =  True
        )
        kube_downstream_dependencies.append(kube_facebook)
    dv360_list = {env["DV360_WAITOA_ADVERTISER_ID"]:'waitoa',env["DV360_INGHAMS_ADVERTISER_ID"]:'inghams'}
    for key,label in dv360_list.items():
        kube_dv360 = KubernetesPodOperator(
            name=f"{label}-dv360-to-bigquery",
            task_id=f"{label}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery",f"dbt-bigquery:dv360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(key,label),
            #base_container_name=f"meltano-{label}-dv360",
            get_logs =  True
        )
        kube_downstream_dependencies.append(kube_dv360)
    ttd_list = {env["TTD_WAITOA_ID"]:'waitoa',env["TTD_INGHAMS_ID"]:'inghams'}
    for key,label in ttd_list.items():
        kube_ttd = KubernetesPodOperator(
            name=f"{label}-ttd-to-bigquery",
            task_id=f"{label}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery",f"dbt-bigquery:ttd_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(key,label),
            #base_container_name=f"meltano-{label}-ttd",
            get_logs =  True,
            execution_timeout=timedelta(minutes=60)
        )
        kube_downstream_dependencies.append(kube_ttd)
        kube_cm360 = KubernetesPodOperator(
            name=f"{label}-cm360-transformation",
            task_id = f"{label}-cm360_transformation",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{label}_models"],
            env_vars = set_env_vars_cm360(label),
        )
        kube_cm360 >> kube_ttd
        kube_downstream_dependencies.append(kube_cm360)
        

        kube_dash = KubernetesPodOperator(
            name=f"{label}-dash-to-bigquery",
            task_id=f"{label}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
            )
        kube_dash_union=KubernetesPodOperator(
            name=f"{label}-dash-union-to-bigquery",
            task_id=f"{label}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(label),
            #base_container_name=f"meltano-{label}-dash",
        )
        



    kube_downstream_dependencies >> kube_dash
    kube_dash >> kube_dash_union 