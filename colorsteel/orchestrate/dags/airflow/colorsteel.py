import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.models import Variable
import pendulum
from kubernetes.client import models as k8s_models
from copy import deepcopy
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import timedelta
from google.cloud import storage


IMAGE = "australia-southeast1-docker.pkg.dev/colorsteel-main/meltano/meltano-colorsteel-main:prod"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_colorsteel_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    start_date_str = (datetime.datetime.now(local_tz) - datetime.timedelta(days=13)).strftime("%Y-%m-%d")
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def set_env_vars_facebook():
    env = get_meltano_env()
    env["BQ_DATASET"] = "facebook_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
    return env


def set_env_vars_cm360():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
    return env


def set_env_vars_pinterest():
    env = get_meltano_env()
    env["BQ_DATASET"] = "pinterest_raw"
    env["BQ_METHOD"] = "batch_job"
    env["TAP_PINTEREST_ADS_END_DATE"] = datetime.datetime.now(local_tz).strftime("%Y-%m-%d")
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'pinterest_transformed'
    return env


def set_env_vars_dv360():
    env = get_meltano_env()
    env["BQ_DATASET"] = "dv360_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
    return env


def set_env_vars_ttd():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ttd_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env


def set_env_vars_ga4():
    env = get_meltano_env()
    env["BQ_DATASET"] = "ga4_raw"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
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


def set_env_vars_dash():
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'colorsteel-main'
    env["DBT_BIGQUERY_DATASET"] = 'dash_table'
    return env


with models.DAG(
    dag_id="colorsteel-meltano-extraction-transformation-dbt",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as dag:

    kube_facebook = KubernetesPodOperator(
        name="colorsteel-facebook-to-bigquery",
        task_id="colorsteel-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", "dbt-bigquery:facebook_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        get_logs=True
    )

    kube_ttd = KubernetesPodOperator(
        name="colorsteel-ttd-to-bigquery",
        task_id="colorsteel-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", "dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        get_logs=True,
        execution_timeout=timedelta(minutes=60)
    )

    kube_cm360 = KubernetesPodOperator(
        name="colorsteel-cm360-to-bigquery",
        task_id="colorsteel-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),
        get_logs=True
    )

    kube_dv360 = KubernetesPodOperator(
        name="colorsteel-dv360-to-bigquery",
        task_id="colorsteel-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", "dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        get_logs=True
    )

    kube_pinterest = KubernetesPodOperator(
        name="colorsteel-pinterest-to-bigquery",
        task_id="colorsteel-pinterest_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-pinterest-ads", "target-bigquery", "--full-refresh", "dbt-bigquery:pinterest_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_pinterest(),
        get_logs=True
    )

    kube_ga4 = KubernetesPodOperator(
        name="colorsteel-ga4-to-bigquery",
        task_id="colorsteel-ga4_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery", "dbt-bigquery:ga4_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ga4(),
        get_logs=True
    )

    kube_dash = KubernetesPodOperator(
        name="colorsteel-dash-to-bigquery",
        task_id="colorsteel-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule='all_done',
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True
    )

    kube_dash_union = KubernetesPodOperator(
        name="colorsteel-dash-union-to-bigquery",
        task_id="colorsteel-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        get_logs=True
    )

    [kube_facebook, kube_ttd, kube_cm360, kube_dv360, kube_pinterest] >> kube_dash >> kube_dash_union >> kube_ga4
