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


IMAGE = "australia-southeast1-docker.pkg.dev/ballance-main/meltano/meltano-ballance-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
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
    meltano_env_unique = Variable.get("meltano_ballance_main", deserialize_json=True)
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
    dag_id="ballance-meltano-google-ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as google_dag:
    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_search_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_google_ads():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_dv_transformed'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
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
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    set_env_task_google_ads_search = PythonOperator(
        task_id="set_env_google_ads_search",
        python_callable=set_env_vars_google_ads_search,
    )
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_google_ads = PythonOperator(
        task_id="set_env_google_ads",
        python_callable=set_env_vars_google_ads,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_dash_search",
        python_callable=set_env_vars_dash_search,
    )
    kube_ga4 = KubernetesPodOperator(
        name="ballance-ga4-to-bigquery",
        task_id="ballance-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
        get_logs=True,
        is_delete_operator_pod=True,
    )
    kube_google_ads_search = KubernetesPodOperator(
        name="ballance-google-ads-search-to-bigquery",
        task_id="ballance-google_ads_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_search(),
        get_logs=True,
        is_delete_operator_pod=True,
    )
    kube_google_ads = KubernetesPodOperator(
        name="ballance-google-ads-to-bigquery",
        task_id="ballance-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "google_ads_dv"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
        base_container_name = "meltano-ballance-google-ads"
    )
    kube_dash = KubernetesPodOperator(
        name="ballance-dash-to-bigquery",
        task_id="ballance-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        trigger_rule= "all_done",
        base_container_name = "meltano-ballance-dash"
        )
    kube_dash_search = KubernetesPodOperator(
        name="ballance-dash-search-to-bigquery",
        task_id="ballance-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        base_container_name = "meltano-ballance-dash-search"
        )
    kube_dash_union = KubernetesPodOperator(
        name="ballance-dash-union-to-bigquery",
        task_id="ballance-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        #base_container_name = "meltano-ballance-dash-union"
        )

    set_env_task_google_ads >> kube_google_ads >> kube_dash
    set_env_task_google_ads_search >> kube_google_ads_search >> kube_dash_search
    kube_dash >> kube_dash_search>> kube_dash_union >>set_env_task_ga4 >> kube_ga4 
       
with models.DAG(
    dag_id="ballance-meltano-extraction-transformation-dbt",
    schedule_interval="0 3 * * *",
    default_args=default_args,
) as dag:
    
    def set_env_vars_facebook(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{brand}'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"]=env[f"TAP_FACEBOOK_{brand}_AIRBYTE_CONFIG_ACCOUNT_ID"]
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env


    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'ballance-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env

    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )

    set_env_task_dv360 = PythonOperator(
        task_id="set_env_dv360",
        python_callable=set_env_vars_dv360,
    )
    set_env_task_ttd = PythonOperator(
        task_id="set_env_ttd",
        python_callable=set_env_vars_ttd,
    )

    set_env_task_dash_search = PythonOperator(
        task_id="set_env_dash_search",
        python_callable=set_env_vars_dash_search,
    )


    kube_cm360 = KubernetesPodOperator(
        name="ballance-cm360-transformation",
        task_id = "ballance-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        env_vars = set_env_vars_cm360(),
        get_logs=True,
        is_delete_operator_pod=True,
    )
    facebook_list=['brand','simp']
    facebook_work={}
    for item in facebook_list:
        
        kube_facebook = KubernetesPodOperator(
            name=f"ballance-{item}-facebook-to-bigquery",
            task_id=f"ballance-{item}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",f"dbt-bigquery:facebook_{item}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(item),
            get_logs=True,
            is_delete_operator_pod=True,
        )
        facebook_work.setdefault(item,kube_facebook)
    kube_dv360 = KubernetesPodOperator(
        name="ballance-dv360-to-bigquery",
        task_id="ballance-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        get_logs = True,
        is_delete_operator_pod=True,
    )

    kube_ttd = KubernetesPodOperator(
        name="ballance-ttd-to-bigquery",
        task_id="ballance-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        get_logs = True,
        execution_timeout=timedelta(minutes=60),
        is_delete_operator_pod=True,
    )


    kube_dash = KubernetesPodOperator(
        name="ballance-dash-to-bigquery",
        task_id="ballance-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        trigger_rule= "all_done",
        base_container_name = "meltano-ballance-dash"
        )
    kube_dash_search = KubernetesPodOperator(
        name="ballance-dash-search-to-bigquery",
        task_id="ballance-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        get_logs=True
        #base_container_name = "meltano-ballance-dash-search"
        )
    kube_dash_union = KubernetesPodOperator(
        name="ballance-dash-union-to-bigquery",
        task_id="ballance-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        #base_container_name = "meltano-ballance-dash-union"
        )
    
    for label,task in facebook_work.items():
        task >> kube_dash
    set_env_task_dv360 >> kube_dv360
    set_env_task_ttd >> kube_ttd 
    set_env_task_cm360 >> kube_cm360
    set_env_task_cm360>>kube_cm360 >> set_env_task_ttd >> kube_ttd
    set_env_task_dash_search >> kube_dash_search
    [kube_dv360,kube_ttd,kube_cm360] >>  kube_dash 
    kube_dash >> kube_dash_search >> kube_dash_union