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


IMAGE = "australia-southeast1-docker.pkg.dev/barfoot-and-thompson-main/meltano/meltano-barfoot-main:prod"


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

# Setting timezone for DAG's start date
start_date = datetime.datetime(2024, 1, 1, tzinfo=local_tz)
start_date_str = start_date.strftime("%Y-%m-%d")
start_date_str = yesterday.strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
ga4_start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_barfoot_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id = "barfoot-meltano-google-ads",
    schedule_interval=' 30 14 * * *',
    default_args=default_args,
)as google_dag:
    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def ser_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
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
        env["TAP_GA4_END_DATE"] = end_date
        return env
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_google_ads_search = PythonOperator(
        task_id="set_env_google_ads_search",
        python_callable=set_env_vars_google_ads_search,
    )
    kube_google_ads_search = KubernetesPodOperator(
        name="barfoot-google-ads-search-to-bigquery",
        task_id="barfoot-google-ads-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads_search(),
    )
    kube_dash_search = KubernetesPodOperator(
        name="barfoot-dash-search-to-bigquery",
        task_id="barfoot-dash-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=ser_env_vars_dash_search(),
    )
    kube_ga4 = KubernetesPodOperator(
        name="barfoot-ga4-to-bigquery",
        task_id="barfoot-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
    )
    kube_dash = KubernetesPodOperator(
        name="barfoot-dash-to-bigquery",
        task_id="barfoot-dash_to_bigquery",
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
        name="barfoot-dash-union-to-bigquery",
        task_id="barfoot-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        #base_container_name=f"meltano-barfoot-dash",
        )
    set_env_task_google_ads_search >> kube_google_ads_search
    set_env_task_ga4 >> kube_ga4
    [kube_google_ads_search] >> kube_dash_search
    kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4 
with models.DAG(
    dag_id="barfoot-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:
    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    

    def set_env_vars_hivestack():
        env = get_meltano_env()
        env["BQ_DATASET"] = "hivestack_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'hivestack_transformed'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        return env

    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def ser_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'barfoot-and-thompson-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env

    set_env_task_cm360 = PythonOperator(
        task_id="set_env_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_hivestack = PythonOperator(
        task_id="set_env_hivestack",
        python_callable=set_env_vars_hivestack,
    )

    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )

    set_env_task_dv360 = PythonOperator(
        task_id="set_env_dv360",
        python_callable=set_env_vars_dv360,
    )
    set_env_task_ttd = PythonOperator(
        task_id="set_env_ttd",
        python_callable=set_env_vars_ttd,
    )
    set_env_task_linkedin = PythonOperator(
        task_id="set_env_linkedin", 
        python_callable=set_env_vars_linkedin,
    )
    kube_hivestack = KubernetesPodOperator(
        name="barfoot-hivestack-to-bigquery",
        task_id="barfoot-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery","dbt-bigquery:hivestack_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_hivestack(),
    )

    kube_linkedin = KubernetesPodOperator(
        name="barfoot-linkedin-to-bigquery",
        task_id="barfoot-linkedin_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run","tap-linkedin-ads","target-bigquery","dbt-bigquery:linkedin_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
    )
    kube_dash_search = KubernetesPodOperator(
        name="barfoot-dash-search-to-bigquery",
        task_id="barfoot-dash-search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=ser_env_vars_dash_search(),
    )

    kube_cm360 = KubernetesPodOperator(
        name="barfoot-cm360-transformation",
        task_id = "barfoot-cm360_transformation",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        env_vars = set_env_vars_cm360(),
    )
    
    kube_facebook = KubernetesPodOperator(
        name="barfoot-facebook-to-bigquery",
        task_id="barfoot-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook()
    )

    kube_dv360 = KubernetesPodOperator(
        name="barfoot-dv360-to-bigquery",
        task_id="barfoot-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360()
    )

    kube_ttd = KubernetesPodOperator(
        name="barfoot-ttd-to-bigquery",
        task_id="barfoot-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd()
    )


    kube_dash = KubernetesPodOperator(
        name="barfoot-dash-to-bigquery",
        task_id="barfoot-dash_to_bigquery",
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
        name="barfoot-dash-union-to-bigquery",
        task_id="barfoot-dash_union_to_bigquery",
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


    set_env_task_cm360 >> kube_cm360
    set_env_task_hivestack >> kube_hivestack
    set_env_task_linkedin >> kube_linkedin

    
    [kube_facebook,kube_dv360,kube_ttd,kube_cm360,kube_linkedin,kube_hivestack] >> kube_dash
    kube_dash >> kube_dash_search >> kube_dash_union