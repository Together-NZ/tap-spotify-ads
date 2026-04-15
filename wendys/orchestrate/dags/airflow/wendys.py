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


IMAGE = "australia-southeast1-docker.pkg.dev/wendys-main/meltano/meltano-wendys-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
ga4_start_date = datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz),
    'retry_delay': timedelta(minutes=30)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_wendys_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=14)
    start_date_str = yesterday.strftime("%Y-%m-%d")

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
with models.DAG(
    dag_id = "wendys-meltano-google-ads",
    schedule_interval="20 13 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=80),
) as google_dag:
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_google_ads():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'   
 
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_START_DATE"] = get_ga4_start_date()
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env
    kube_tiktok = KubernetesPodOperator(
        name="wendys-tiktok-to-bigquery",
        task_id="wendys-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","--full-refresh","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        
  
    )
    kube_dash_search=KubernetesPodOperator(
        name="wendys-dash_search-to-bigquery",
        task_id="wendys-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
    )
    kube_ga4 = KubernetesPodOperator(
            name = "wendys-ga4-to-bigquery",
            task_id = "wendys-ga4_to_bigquery",
            namespace = "composer-user-workloads",
            image = IMAGE,
            arguments = ["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4()),
    kube_dash = KubernetesPodOperator(
        name="wendys-dash-to-bigquery",
        task_id="wendys-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule='all_done',
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        
        )
    kube_google_ads_search=KubernetesPodOperator(
        name="wendys-google-ads-search-to-bigquery",
        task_id="wendys-google-ads_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule='all_done',
        arguments=["--environment=prod", "invoke","dbt-bigquery:google_ads_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            
        ),
        env_vars=set_env_vars_google_ads(),
    )

    kube_dash_union = KubernetesPodOperator(
        name="wendys-dash-union-to-bigquery",
        task_id="wendys-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )
    comparison_trigger_tiktok = ComparisonTrigger(
        project_name="wendys-main",
        destination_table="tiktok_transformed",
        table_name="tiktok",
        source_name="tiktok",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_wendys_main",
        project_id=env["PROJECT_ID"]
        )
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
    kube_tiktok >> task_tiktok_comparison
    kube_tiktok >> kube_google_ads_search >> kube_dash >> kube_dash_search >> kube_dash_union >> kube_ga4
    
    
    
    
    
with models.DAG(
    dag_id="wendys-meltano-extraction-transformation-dbt",
    schedule_interval="0 6 * * *",
    default_args=default_args,
     dagrun_timeout=timedelta(minutes=80),
) as dag:
    def set_env_vars_hivestack():
        env = get_meltano_env()
        env["BQ_DATASET"] = "hivestack_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'hivestack_transformed'
        return env
    def set_env_vars_snapchat():
        env = get_meltano_env()
        env["BQ_DATASET"] = "snapchat_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'snapchat_transformed'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env
   
        
    
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'wendys-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"]=get_ttd_start_date()
        return env
  
    set_env_task_snapchat = PythonOperator(
        task_id="set_env_snapchat",
        python_callable=set_env_vars_snapchat,
    )
    set_env_task_hivestack = PythonOperator(
        task_id="set_env_hivestack",
        python_callable=set_env_vars_hivestack,
    )
    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )

    env = get_meltano_env()

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
    comparison_trigger_facebook = ComparisonTrigger(
        project_name="wendys-main",
        destination_table="facebook_transformed",
        table_name="facebook",
        source_name="meta",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_wendys_main",
        project_id=env["PROJECT_ID"]
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

    kube_snapchat = KubernetesPodOperator(
        name="wendys-snapchat-to-bigquery",
        task_id="wendys-snapchat_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-snapchat-ads", "target-bigquery","dbt-bigquery:snapchat_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_snapchat(),
        #base_container_name= "meltano-aia-snapchat"
    )
    kube_hivestack = KubernetesPodOperator(
        name="wendys-hivestack-to-bigquery",
        task_id="wendys-hivestack_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery","dbt-bigquery:hivestack_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_hivestack(),
        #base_container_name= "meltano-aia-hivestack"
    )

        

          
    kube_dash_union = KubernetesPodOperator(
        name="wendys-dash-union-to-bigquery",
        task_id="wendys-dash_union_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )


    kube_facebook = KubernetesPodOperator(
        name="wendys-facebook-to-bigquery",
        task_id="wendys-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        base_container_name= "meltano-aia-facebook"
    )
    kube_dv360 = KubernetesPodOperator(
        name="wendys-dv360-to-bigquery",
        task_id="wendys-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        
       
    )
    kube_cm360 = KubernetesPodOperator(
        name="wendys-cm360-to-bigquery",
        task_id="wendys-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),

       
    )
    
    kube_ttd = KubernetesPodOperator(
        name="wendys-ttd-to-bigquery",
        task_id="wendys-ttd_to_bigquery",
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
        name="wendys-dash-to-bigquery",
        task_id="wendys-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        trigger_rule='all_done',
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
        
        
        )

    set_env_task_facebook >> kube_facebook >> task_facebook_comparison
    set_env_task_dv360 >> kube_dv360
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd 
    set_env_task_hivestack >> kube_hivestack
    set_env_task_snapchat >> kube_snapchat
    #kube_google_ads_search >> kube_dash_search
    [kube_facebook,kube_dv360,kube_cm360,kube_ttd,kube_hivestack,kube_snapchat] >> kube_dash
    kube_dash >> kube_dash_union