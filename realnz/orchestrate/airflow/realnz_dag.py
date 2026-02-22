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

from datetime import timedelta, datetime as dt
import json

IMAGE = "australia-southeast1-docker.pkg.dev/real-nz-main/meltano/meltano-realnz-main:prod"

log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = dt.now(local_tz) - timedelta(days=1)
ga4_start_date = dt.now(local_tz) - timedelta(days=30)

default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': datetime.timedelta(minutes=30),
    "start_date": dt(2025, 1, 1, tzinfo=local_tz),
}


def get_meltano_env():
    meltano_env_unique = Variable.get("meltano_realnz_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret", deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=3)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_meta_start_date():
    return (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=3)
    ).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
def set_env_vars_hivestack(_id, label):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"hivestack_raw__{label}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"hivestack_transformed__{label}"
    env["TAP_HIVESTACK_REPORT_ID"] = _id
    return env

def set_env_vars_ga4(_id, label, _type):
    env = get_meltano_env()
    if _type == "goal":
        env["GA4_REPORTS"] = "./report.json"
        env["GA4_GOAL"] = "goal"
    else:
        env["GA4_REPORTS"] = "./ecommerce_report.json"
        env["GA4_GOAL"] = "ecommerce_goal"
    env["BQ_DATASET"] = f"ga4_raw__{label}"
    env["BQ_METHOD"] = "gcs_stage"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"ga4_transformed__{label}"

    developer_creds = Credentials(
        None,
        refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
        client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
    )
    developer_creds.refresh(Request())
    env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
    env["TAP_GA4_PROPERTY_ID"] = _id
    env["TAP_GA4_START_DATE"] = get_ga4_start_date()
    return env

def set_env_vars_facebook(account_id, value):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"facebook_raw__{value}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"facebook_transformed__{value}"
    env["TAP_FACEBOOK_ACCOUNT_ID"] = account_id
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_ACCOUNT_ID"] = account_id
    env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"]=get_meta_start_date()
    return env

def set_env_vars_cm360(value):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"cm360_transformed__{value}"
    return env

def set_env_vars_dv360(account_id, value):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"dv360_raw__{value}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"dv360_transformed__{value}"
    env["TAP_DV360_ADVERTISER_ID"] = account_id
    return env

def set_env_vars_ttd(key, value):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"ttd_raw__{value}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"ttd_transformed__{value}"
    env["TAP_TTD_ADVERTISER_ID"] = key
    env["TAP_TTD_START_DATE"] = get_ttd_start_date()
    return env

def set_env_vars_google_ads(value):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_dv_transformed__{value}"
    return env

def set_env_vars_dash(value):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"dash_table__{value}"
    return env

def set_env_vars_tiktok(_id, value):
    env = get_meltano_env()
    env["BQ_DATASET"] = f"tiktok_raw__{value}"
    env["BQ_METHOD"] = "batch_job"
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"tiktok_transformed__{value}"
    env["TAP_TIKTOK_ADVERTISER_ID"] = _id
    return env

def set_env_vars_google_ads_search(value):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"google_ads_search_transformed__{value}"
    return env

def set_env_vars_dash_search(value):
    env = get_meltano_env()
    env["DBT_BIGQUERY_METHOD"] = "oauth"
    env["DBT_BIGQUERY_PROJECT"] = "real-nz-main"
    env["DBT_BIGQUERY_DATASET"] = f"dash_table_search__{value}"
    return env
with models.DAG(
    dag_id="realnz-meltano-google_ads",
    schedule_interval="0 14 * * *",
    default_args=default_args,
) as google_dag:
    env = get_meltano_env()
    ga4_tasks = []  
    dash_union_tasks = []   
    kube_dash_by_label = {}
    kube_dash_search_by_label = {}
    kube_dash_union_by_label = {}
    ga4_type_list = ["ecommerce", "goal"]
    per_label_tasks_search = {}   # upstreams per label for dash_search
    tiktok_list = {env["TAP_TIKTOK_ADVERTISER_ID_MOUNTAIN"]: "mountain"}
    for key, label in tiktok_list.items():
        kube_tiktok = KubernetesPodOperator(
            name=f"realnz-tiktok-to-bigquery-{label}",
            task_id=f"realnz_tiktok_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","--full-refresh", f"dbt-bigquery:tiktok_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_tiktok(key, label),
        )
    list = ["mountain", "tourism"]
    for label in list:
        kube_google_ads_search = KubernetesPodOperator(
            name=f"realnz-google-ads-search-to-bigquery-{label}",
            task_id=f"realnz_google_ads_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_google_ads_search(label),
        )
        per_label_tasks_search.setdefault(label, []).append(kube_google_ads_search)
    for label in ["realnz"]:
        kube_google_ads = KubernetesPodOperator(
            name=f"realnz-google-ads-to-bigquery-{label}",
            task_id=f"realnz-google_ads_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_dv__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_google_ads(label),
        )
    dash_list = ["mountain", "tourism"]
    for label in dash_list:
        kube_google_ads_demand = KubernetesPodOperator(
            name=f"realnz-google-ads-demand-to-bigquery-{label}",
            task_id=f"realnz-google_ads_demand_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_demand__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_google_ads_search(label),
        )
        kube_dash_union = KubernetesPodOperator(
            name=f"realnz-dash-union-to-bigquery-{label}",
            task_id=f"realnz-dash_union_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash(label),
        )
        kube_dash = KubernetesPodOperator(
            name=f"realnz-dash-to-bigquery-{label}",
            task_id=f"realnz-dash_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash(label),
        )
        if label=="mountain":
            kube_tiktok >> kube_dash
        kube_google_ads_demand >> kube_dash
        kube_dash_by_label[label] = kube_dash
        kube_dash_union_by_label[label] = kube_dash_union
        dash_union_tasks.append(kube_dash_union)
    dash_search_list = ["mountain", "tourism"]
    for label in dash_search_list:
        kube_dash_search = KubernetesPodOperator(
            name=f"realnz-dash-search-to-bigquery-{label}",
            task_id=f"realnz-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash_search(label),
        )
        for upstream_task in per_label_tasks_search.get(label, []):
            upstream_task >> kube_dash_search
        kube_dash_search_by_label[label] = kube_dash_search

   # GA4 tasks (collect only; wire later)
    ga4_list = {env["TAP_GA4_PROPERTY_ID_MOUNTAIN"]: "mountain",
                env["TAP_GA4_PROPERTY_ID_TOURISM"]: "tourism"}
    for key, label in ga4_list.items():
        for _type in ga4_type_list:
            kube_ga4 = KubernetesPodOperator(
                name=f"realnz-ga4-to-bigquery-{label}-{_type}",
                task_id=f"realnz_ga4_to_bigquery_{label}_{_type}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "run", "tap-ga4", "target-bigquery", f"dbt-bigquery:ga4_{label}_{_type}_models"],
                container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
                env_vars=set_env_vars_ga4(key, label, _type),
            )
            ga4_tasks.append(kube_ga4)
    # ===== Per-label strict order: dash -> dash_search -> dash_union =====
    for label in ["mountain", "tourism"]:
    
            kube_google_ads >> kube_google_ads_demand >>kube_dash_by_label[label] >> kube_dash_search_by_label[label] >> kube_dash_union_by_label[label]
    # ===== Global order: ALL dash_union must complete before ANY GA4 starts =====
    for label in ["mountain", "tourism"]:
        kube_dash_union_by_label[label] >> ga4_tasks   
with models.DAG(
    dag_id="realnz-meltano-extraction-transformation-dbt",
    schedule_interval="0 4 * * *",
    default_args=default_args,
) as dag:

    env = get_meltano_env()

    # Containers for wiring
    per_label_tasks = {}          # non-GA4 upstreams per label for dash

    kube_dash_by_label = {}
    kube_dash_search_by_label = {}
    kube_dash_union_by_label = {}

    dash_union_tasks = []         # collect ALL unions (for global dep)
              # collect ALL GA4 tasks

    # Static lists/inputs
    hivestack_list = {env["TAP_HIVESTACK_REPORT_ID_MOUNTAIN"]: "mountain",
                      env["TAP_HIVESTACK_REPORT_ID_TOURISM"]: "tourism"}


    # Hivestack
    for key, label in hivestack_list.items():
        kube_hivestack = KubernetesPodOperator(
            name=f"realnz-hivestack-to-bigquery-{label}",
            task_id=f"realnz_hivestack_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery", f"dbt-bigquery:hivestack_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_hivestack(key, label),
        )
        per_label_tasks.setdefault(label, []).append(kube_hivestack)
        #per_label_tasks.setdefault(label, []).append(kube_tiktok)
    # Facebook + CM360 (+ optional TTD for tourism)
    facebook_list = {env["TAP_FACEBOOK_ACCOUNT_ID_MOUNTAIN"]: "mountain",
                     env["TAP_FACEBOOK_ACCOUNT_ID_TOURISM"]: "tourism"}
    for key, label in facebook_list.items():
        kube_facebook = KubernetesPodOperator(
            name=f"realnz-facebook-to-bigquery-{label}",
            task_id=f"realnz_facebook_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery", f"dbt-bigquery:facebook_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_facebook(key, label),
        )
        per_label_tasks.setdefault(label, []).append(kube_facebook)

        kube_cm360 = KubernetesPodOperator(
            name=f"realnz-cm360-to-bigquery-{label}",
            task_id=f"realnz_cm360_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", f"dbt-bigquery:cm360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_cm360(label),
        )
        per_label_tasks.setdefault(label, []).append(kube_cm360)

        if label == "tourism":
            ttd_list = {env["TAP_TTD_TOURISM"]: "tourism",env["TAP_TTD_MOUNTAIN"]: "mountain"}
            for key2, label2 in ttd_list.items():
                kube_ttd = KubernetesPodOperator(
                    name=f"realnz-ttd-to-bigquery-{label2}",
                    task_id=f"realnz-ttd_to_bigquery_{label2}",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery", f"dbt-bigquery:ttd_{label2}_models"],
                    container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
                    env_vars=set_env_vars_ttd(key2, label2),
                    execution_timeout=timedelta(minutes=60)
                )
                kube_cm360 >> kube_ttd
                per_label_tasks.setdefault(label2, []).append(kube_ttd)

    # DV360 + Google Ads Search (search-specific upstreams)
    dv360_list = {env["TAP_DV360_ACCOUNT_ID_MOUNTAIN"]: "mountain",
                  env["TAP_DV360_ACCOUNT_ID_TOURISM"]: "tourism"}
    for key, label in dv360_list.items():
        kube_dv360 = KubernetesPodOperator(
            name=f"realnz-dv360-to-bigquery-{label}",
            task_id=f"realnz_dv360_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery", f"dbt-bigquery:dv360_{label}_models"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dv360(key, label),
        )
        per_label_tasks.setdefault(label, []).append(kube_dv360)


    # TikTok


    # Google Ads (DV) - global

        # If this should feed into a specific label's dash, add it to that label as needed:
        # per_label_tasks.setdefault("mountain", []).append(kube_google_ads)

    # Dash per label
    dash_list = ["mountain", "tourism"]
    for label in dash_list:
        kube_dash_union = KubernetesPodOperator(
            name=f"realnz-dash-union-to-bigquery-{label}",
            task_id=f"realnz-dash_union_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_union__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash(label),
        )
        kube_dash = KubernetesPodOperator(
            name=f"realnz-dash-to-bigquery-{label}",
            task_id=f"realnz-dash_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            trigger_rule="all_done",
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash(label),
        )

        # non-GA4 sources -> dash
        for upstream_task in per_label_tasks.get(label, []):
            upstream_task >>  kube_dash
        kube_dash_by_label[label] = kube_dash
        kube_dash_union_by_label[label] = kube_dash_union
        dash_union_tasks.append(kube_dash_union)

    # Dash search per label
    dash_search_list = ["mountain", "tourism"]
    for label in dash_search_list:
        kube_dash_search = KubernetesPodOperator(
            name=f"realnz-dash-search-to-bigquery-{label}",
            task_id=f"realnz-dash_search_to_bigquery_{label}",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"dash_table_search__{label}"],
            container_resources=k8s_models.V1ResourceRequirements(limits={"memory": "1000M", "cpu": "500m"}),
            env_vars=set_env_vars_dash_search(label),
        )



        kube_dash_search_by_label[label] = kube_dash_search

 

    # ===== Per-label strict order: dash -> dash_search -> dash_union =====
    for label in ["mountain", "tourism"]:
        kube_dash_by_label[label] >> kube_dash_search_by_label[label] >> kube_dash_union_by_label[label]

    # ===== Global order: ALL dash_union must complete before ANY GA4 starts =====
    for label in ["mountain", "tourism"]:
        kube_dash_union_by_label[label] 
