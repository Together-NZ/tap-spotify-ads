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
import logging
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.cloud import storage
from comparison_package import ComparisonTrigger


IMAGE = "australia-southeast1-docker.pkg.dev/volvo-main/meltano/meltano-volvo-main:prod"
local_tz = pendulum.timezone("Pacific/Auckland")
log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
local_tz = pendulum.timezone("Pacific/Auckland")

default_args = {
    "retries": 3,
    "retry_delay": datetime.timedelta(hours=2),
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2026, 4, 1, tzinfo=local_tz)
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_volvo_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = meltano_env_unique
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)


def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def set_env_vars_google_ads_search(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = "google_ads_search"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
    return env


def set_env_vars_hivestack(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"hivestack_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'hivestack_transformed__{label}'
    return env


def set_env_vars_facebook(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'facebook_transformed__{label}'
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = env[f"{label}_TAP_FACEBOOK_ACCOUNT_ID"]
    return env


def set_env_vars_ga4(value):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"ga4_raw__{value}"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{value}'
    env["TAP_GA4_PROPERTY_ID"] = id
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
    env["TAP_GA4_PROPERTY_ID"] = env[f'{value}_TAP_GA4_PROPERTY_ID']
    return env


def set_env_vars_linkedin(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"linkedin_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'linkedin_transformed__{label}'
    return env


def set_env_vars_dv360(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'dv360_transformed__{label}'
    return env


def set_env_vars_cm360(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = "cm360_raw"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'cm360_transformed__{label}'
    return env


def set_env_vars_ttd(label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"ttd_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'ttd_transformed__{label}'
    return env


def set_env_vars_dash(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
    return env


def set_env_vars_dash_search(label):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = 'oauth'
    env["DBT_BIGQUERY_PROJECT"] = 'volvo-main'
    env["DBT_BIGQUERY_DATASET"] = f'dash_table_search__{label}'
    return env


# ---------------------------------------------------------------------------
# DAG 1: Google Ads + GA4 (schedule: 14:00 NZST daily)
# Flow: google_ads >> dash >> dash_search >> dash_union >> ga4
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="volvo-google-ads-ga4",
    schedule_interval="0 14 * * *",
    default_args=default_args
) as dag_google:

    brands = ['volvo']
    for brand in brands:
        kube_google_ads = KubernetesPodOperator(
            name=f"{brand}-google-ads-to-bigquery",
            task_id=f"{brand}-google-ads_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(brand),
            get_logs=True
        )

        kube_dash = KubernetesPodOperator(
            name=f"{brand}-dash-to-bigquery",
            task_id=f"{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"{brand}-dash-search-to-bigquery",
            task_id=f"{brand}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"{brand}-dash-union-to-bigquery",
            task_id=f"{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )

        kube_ga4 = KubernetesPodOperator(
            name=f"{brand}-ga4-to-bigquery",
            task_id=f"{brand}-ga4_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery", f"dbt-bigquery:ga4_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4(brand),
        )


        kube_google_ads >> kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4


# ---------------------------------------------------------------------------
# DAG 2: Social / Display / Programmatic (schedule: 05:00 NZST daily)
# Flow: [facebook, dv360, cm360, linkedin, ttd, hivestack] >> dash >> dash_search >> dash_union
# ---------------------------------------------------------------------------
with models.DAG(
    dag_id="volvo-social-display-programmatic",
    schedule_interval="0 5 * * *",
    default_args=default_args
) as dag_social:

    brands = ['volvo']
    for brand in brands:
        kube_facebook = KubernetesPodOperator(
            name=f"{brand}-facebook-to-bigquery",
            task_id=f"{brand}-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery",
                        "--full-refresh",
                        f"dbt-bigquery:facebook_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(brand),
            get_logs=True
        )

        kube_dv360 = KubernetesPodOperator(
            name=f"{brand}-dv360-to-bigquery",
            task_id=f"{brand}-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", f"dbt-bigquery:dv360_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(brand),
        )

        kube_cm360 = KubernetesPodOperator(
            name=f"{brand}-cm360-to-bigquery",
            task_id=f"{brand}-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360(brand),
        )

        kube_linkedin = KubernetesPodOperator(
            name=f"{brand}-linkedin-to-bigquery",
            task_id=f"{brand}-linkedin_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery", f"dbt-bigquery:linkedin_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_linkedin(brand),
            get_logs=True
        )

        kube_ttd = KubernetesPodOperator(
            name=f"{brand}-ttd-to-bigquery",
            task_id=f"{brand}-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", f"dbt-bigquery:ttd_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(brand),
        )

        kube_hivestack = KubernetesPodOperator(
            name=f"{brand}-hivestack-to-bigquery",
            task_id=f"{brand}-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", f"dbt-bigquery:hivestack_{brand}_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(brand),
            get_logs=True
        )

        kube_dash = KubernetesPodOperator(
            name=f"{brand}-dash-to-bigquery",
            task_id=f"{brand}-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule='all_done',
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )

        kube_dash_search = KubernetesPodOperator(
            name=f"{brand}-dash-search-to-bigquery",
            task_id=f"{brand}-dash_search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(brand),
        )

        kube_dash_union = KubernetesPodOperator(
            name=f"{brand}-dash-union-to-bigquery",
            task_id=f"{brand}-dash_union_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{brand}"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(brand),
        )
        env=get_meltano_env()
        comparison_trigger_facebook = ComparisonTrigger(
            project_name="volvo-main",
            destination_table="facebook_transformed__volvo",
            table_name="facebook__volvo",
            source_name="meta",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_volvo_main",
            project_id=env["PROJECT_ID"]
            )
        comparison_trigger_linkedin = ComparisonTrigger(
            project_name="volvo-main",
            destination_table="linkedin_transformed__volvo",
            table_name="linkedin__volvo",
            source_name="linkedin",
            start_date=comparison_start_date,
            end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
            secret_name="airflow-variables-meltano_volvo_main",
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
        [kube_facebook, kube_dv360, kube_cm360, kube_linkedin, kube_ttd, kube_hivestack] >> kube_dash >> kube_dash_search >> kube_dash_union
