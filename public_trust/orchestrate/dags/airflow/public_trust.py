from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import storage 
from airflow.operators.dagrun_operator import TriggerDagRunOperator


IMAGE = "australia-southeast1-docker.pkg.dev/public-trust-main/meltano/meltano-public-trust-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
today = datetime.datetime.now(local_tz)
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_public_trust_main", deserialize_json=True)
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
    dag_id="public-trust-meltano-extraction-transformation-dbt",
    schedule_interval="0 3 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    def set_env_vars_google_ads_dv():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_dv_transformed'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def ser_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'public-trust-main'
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
    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_google_ads_dv = PythonOperator(
        task_id="set_env_google_ads_dv",
        python_callable=set_env_vars_google_ads_dv,
    )
    set_env_task_google_ads_search = PythonOperator(
        task_id="set_env_google_ads_search",
        python_callable=set_env_vars_google_ads_search,
    )
    set_env_task_ttd = PythonOperator(
        task_id="set_env_ttd",
        python_callable=set_env_vars_ttd,
    )
    kube_google_ads_dv = KubernetesPodOperator(
        name="public-trust-google-ads-dv-to-bigquery",
        task_id="public-trust-google-ads-dv_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","google_ads_dv"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_dv(),
    )
    kube_google_ads_search = KubernetesPodOperator(
        name="public-trust-google-ads-search-to-bigquery",
        task_id="public-trust-google-ads-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","google_ads_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_search(),
    )
    kube_dash_search = KubernetesPodOperator(
        name="public-trust-dash-search-to-bigquery",
        task_id="public-trust-dash-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=ser_env_vars_dash_search(),
    )
    kube_ga4 = KubernetesPodOperator(
        name="public-trust-ga4-to-bigquery",
        task_id="public-trust-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
    )
    kube_cm360 = KubernetesPodOperator(
        name="public-trust-cm360-transformation",
        task_id = "public-trust-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        env_vars = set_env_vars_cm360(),
    )
    
    kube_facebook = KubernetesPodOperator(
        name="public-trust-facebook-to-bigquery",
        task_id="public-trust-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook()
    )

    kube_ttd = KubernetesPodOperator(
        name="public-trust-ttd-to-bigquery",
        task_id="public-trust-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        execution_timeout=timedelta(minutes=60)
    )


    kube_dash = KubernetesPodOperator(
        name="public-trust-dash-to-bigquery",
        task_id="public-trust-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        trigger_rule="all_done",
        #base_container_name=f"meltano-barfoot-dash",
        )
    kube_dash_union = KubernetesPodOperator(
        name="public-trust-dash-union-to-bigquery",
        task_id="public-trust-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        #base_container_name=f"meltano-barfoot-dash",
        )

    set_env_task_facebook >> kube_facebook
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd 
    set_env_task_google_ads_search >> kube_google_ads_search
    set_env_task_ga4 >> kube_ga4
    set_env_task_cm360 >> kube_cm360
    set_env_task_google_ads_dv >> kube_google_ads_dv
    [kube_google_ads_search] >> kube_dash_search
    
    [kube_facebook,kube_ttd,kube_cm360,kube_google_ads_dv] >> kube_dash
    kube_dash >> kube_dash_search >> kube_dash_union