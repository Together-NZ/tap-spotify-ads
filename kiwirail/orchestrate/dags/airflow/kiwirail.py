import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import timedelta
import json


IMAGE = "australia-southeast1-docker.pkg.dev/kiwirail-main/meltano/meltano-kiwirail-main:prod"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2026, 1, 1, tzinfo=local_tz),
}

BRANDS = ["interislander", "great_journey"]


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_kiwirail_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=7)
    meltano_env["START_DATE"] = yesterday.strftime("%Y-%m-%d")
    meltano_env["BQ_METHOD"] = "batch_job"
    return deepcopy(meltano_env)


def get_facebook_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


# ==========================================================================
# DAG 1: Google Ads + GA4
# ==========================================================================
with models.DAG(
    dag_id="kiwirail-meltano-google-ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as dag_google_ads:

    def set_env_vars_google_ads(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"google_ads_transformed__{brand}"
        return env

    def set_env_vars_ga4(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"ga4_raw__{brand}"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{brand}"
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        env["TAP_GA4_PROPERTY_ID"] = env[f"TAP_GA4_PROPERTY_ID_{brand.upper()}"]
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        return env

    def set_env_vars_dash_google(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
        return env

    def set_env_vars_dash_search_google(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{brand}"
        return env

    for brand in BRANDS:
        set_env_google_ads = PythonOperator(
            task_id=f"set_env_google_ads_{brand}",
            python_callable=set_env_vars_google_ads,
            op_args=[brand],
        )

        kube_google_ads = KubernetesPodOperator(
            name=f"kiwirail-google-ads-{brand}-to-bigquery",
            task_id=f"kiwirail-google_ads_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads(brand),
            get_logs=True,
        )

        kube_ga4 = KubernetesPodOperator(
            name=f"kiwirail-{brand}-ga4-to-bigquery",
            task_id=f"kiwirail-{brand}-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery", f"dbt-bigquery:ga4_goal_a_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"kiwirail-dash-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_google(brand),
            trigger_rule="all_done",
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"kiwirail-dash-search-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_search_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search_google(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"kiwirail-dash-union-{brand}-to-bigquery",
            task_id=f"kiwirail-dash_union_{brand}_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_google(brand),
            get_logs=True,
        )

        set_env_google_ads >> kube_google_ads >> kube_dash
        kube_ga4 >> kube_dash
        kube_dash >> kube_dash_search >> kube_dash_union


# ==========================================================================
# DAG 2: Main extraction + transformation (Facebook, DV360)
# ==========================================================================
with models.DAG(
    dag_id="kiwirail-meltano-extraction-transformation-dbt",
    schedule_interval="0 5 * * *",
    default_args=default_args,
) as dag:

    def set_env_vars_facebook(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"facebook_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{brand}"
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"] = get_facebook_start_date()
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[f"TAP_FACEBOOK_{brand.upper()}_AIRBYTE_CONFIG_ACCOUNT_ID"]
        return env

    def set_env_vars_dv360(brand):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"dv360_raw__{brand}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{brand}"
        return env

    def set_env_vars_dash(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"dash_table__{brand}"
        return env

    def set_env_vars_dash_search(brand):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = "oauth"
        env["DBT_BIGQUERY_PROJECT"] = "kiwirail-main"
        env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{brand}"
        return env

    for brand in BRANDS:
        kube_facebook = KubernetesPodOperator(
            name=f"kiwirail-{brand}-facebook-to-bigquery",
            task_id=f"kiwirail-{brand}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", f"dbt-bigquery:facebook_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(brand),
            get_logs=True,
        )

        kube_dv360 = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dv360-to-bigquery",
            task_id=f"kiwirail-{brand}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", f"dbt-bigquery:dv360_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(brand),
            get_logs=True,
        )

        kube_dash = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            trigger_rule="all_done",
            get_logs=True,
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-search-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
            get_logs=True,
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"kiwirail-{brand}-dash-union-to-bigquery",
            task_id=f"kiwirail-{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
            get_logs=True,
        )

        [kube_facebook, kube_dv360] >> kube_dash >> kube_dash_search >> kube_dash_union
