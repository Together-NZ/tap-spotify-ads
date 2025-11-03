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


IMAGE = "australia-southeast1-docker.pkg.dev/aia-nz-main/meltano/meltano-aia-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)

local_tz = pendulum.timezone("Pacific/Auckland")
yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=1)
ga4_start_date = datetime.datetime.now(local_tz) - datetime.timedelta(days=30)
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
ga4_start_date_str = ga4_start_date.strftime("%Y-%m-%d")

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("aia_nz_meltano", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}
    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"
    meltano_env_copy = deepcopy(meltano_env)
    return meltano_env_copy
with models.DAG(
    dag_id = 'aia-meltano_google_ads',
    schedule_interval="30 13 * * *",
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=80),
) as dag_google_ads:
    def set_env_vars_google_ads_search(label):
        env = get_meltano_env()
        env["BQ_DATASET"] = f"google_ads_{label}"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = f'google_ads_search_transformed__{label}'
        
        return env

    def set_env_vars_google_ads():
        env = get_meltano_env()
        env["BQ_DATASET"] = "google_ads_dv_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_dv_transformed'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    
    def set_env_vars_ga4():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ga4_raw"
        env["BQ_METHOD"] = "gcs_stage"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'ga4_transformed'   
 
        developer_creds = Credentials(
            None,
            refresh_token=env["TAP_GA4_OAUTH_CREDENTIALS_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_ID"],
            client_secret=env["TAP_GA4_OAUTH_CREDENTIALS_CLIENT_SECRET"],
        )
        developer_creds.refresh(Request())
        env["TAP_GA4_START_DATE"] = ga4_start_date_str
        env["TAP_GA4_OAUTH_CREDENTIALS_ACCESS_TOKEN"] = developer_creds.token
        return env
    set_env_task_ga4 = PythonOperator(
        task_id="set_env_ga4",
        python_callable=set_env_vars_ga4,
    )
    set_env_task_google_ads = PythonOperator(
        task_id="set_env_google_ads",
        python_callable=set_env_vars_google_ads,
    )

    kube_ga4 = KubernetesPodOperator(
            name = "aia-ga4-to-bigquery",
            task_id = "aia-ga4_to_bigquery",
            namespace = "composer-user-workloads",
            image = IMAGE,
            arguments = ["--environment=prod", "run", "tap-ga4", "target-bigquery","dbt-bigquery:ga4_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ga4()),

    kube_google_ads = KubernetesPodOperator(
        name="aia-google-ads-to-bigquery",
        task_id="aia-google_ads_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "google_ads_dv"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_google_ads(),
        
        
    )
    kube_dash_union  = KubernetesPodOperator(
        name = "aia-dash-union-to-bigquery",
        task_id = "aia-dash_union_to_bigquery",
        namespace = "composer-user-workloads",
        image = IMAGE,
        arguments = ["--environment=prod", "invoke", "dbt-bigquery","run", "--select", "dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )
    env = get_meltano_env()
    search_list = {}
    google_ads_search_list = ['brand_search_raw','dt']
    key_list = ['marketing','brand']
    for label in google_ads_search_list:
        if label == 'dt':
            env['DBT_BIGQUERY_DATASET'] = 'google_ads_search_transformed__marketing'
            key = 'marketing'
        else:
            env['DBT_BIGQUERY_DATASET'] = 'google_ads_search_transformed__brand' 
            key = 'brand'
        kube_google_ads_search = KubernetesPodOperator(
                name=f"aia-google-ads-search-to-bigquery-{key}",
                task_id=f"aia-google_ads_search_to_bigquery-{key}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", f"dbt-bigquery:google_ads_{key}_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_google_ads_search(key),
                
                
            )
        search_list.setdefault(key,[]).append(kube_google_ads_search)
    kube_dash_search = KubernetesPodOperator(
        name="aia-dash-search-to-bigquery",
        task_id="aia-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        
        
    )

    kube_dash = KubernetesPodOperator(
        name="aia-dash-to-bigquery",
        task_id="aia-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule = 'all_done',
        env_vars=set_env_vars_dash(),
        
        
        )
    for index in key_list:
        for task in search_list.get(index,[]):
            task >> kube_dash_search
    set_env_task_google_ads >> kube_google_ads >> kube_google_ads_search
    set_env_task_ga4 >> kube_ga4
    kube_google_ads_search >> kube_dash_search
    kube_dash_search >> kube_dash >> kube_dash_union >>set_env_task_ga4>>kube_ga4 
    
with models.DAG(
    dag_id="aia-meltano-extraction-transformation-dbt",
    schedule_interval="0 6 * * *",
    default_args=default_args,
     dagrun_timeout=timedelta(minutes=80),
) as dag:
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_outbrain():
        env = get_meltano_env()
        env["BQ_DATASET"] = "outbrain_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth' 
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'outbrain_transformed'
        return env
    def set_env_vars_tiktok():
        env = get_meltano_env()
        env["BQ_DATASET"] = "tiktok_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'tiktok_transformed'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "cm360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env


    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        return env

    def set_env_vars_linkedin():
        env=get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'aia-nz-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env


    
    set_env_task_outbrain = PythonOperator(
        task_id="set_env_outbrain",
        python_callable=set_env_vars_outbrain,
    )
    set_env_task_tiktok = PythonOperator(
        task_id="set_env_tiktok",
        python_callable=set_env_vars_tiktok,
    )
    set_env_task_facebook = PythonOperator(
        task_id="set_env_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_linkedin = PythonOperator(
        task_id="set_env_linkedin",
        python_callable=set_env_vars_linkedin,
    )

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

    
    kube_linkedin_ads=KubernetesPodOperator(
        name = "aia-linkedin-to-bigquery",
        task_id = "aia-linkedin_to_bigquery",
        namespace = "composer-user-workloads",
        image = IMAGE,
        arguments = ["--environment=prod", "run", "tap-linkedin-ads", "target-bigquery","dbt-bigquery:linkedin_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_linkedin(),
    )
    env = get_meltano_env()
    kube_dash_union  = KubernetesPodOperator(
        name = "aia-dash-union-to-bigquery",
        task_id = "aia-dash_union_to_bigquery",
        namespace = "composer-user-workloads",
        image = IMAGE,
        arguments = ["--environment=prod", "invoke", "dbt-bigquery","run", "--select", "dash_union"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash(),
    )
        

          
        
    kube_tiktok = KubernetesPodOperator(
        name="aia-tiktok-to-bigquery",
        task_id="aia-tiktok_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-tiktok", "target-bigquery","dbt-bigquery:tiktok_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_tiktok(),
        
  
    )
    kube_outbrain = KubernetesPodOperator(
        name = "aia-outbrain-to-bigquery",
        task_id = "aia-outbrain_to_bigquery",
        namespace = "composer-user-workloads",
        image = IMAGE,
        arguments = ["--environment=prod", "run", "tap-outbrain", "target-bigquery","dbt-bigquery:outbrain_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_outbrain(),
       
    )
    kube_facebook = KubernetesPodOperator(
        name="aia-facebook-to-bigquery",
        task_id="aia-facebook_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
                container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook(),
        #base_container_name= "meltano-aia-facebook"
    )
    kube_dv360 = KubernetesPodOperator(
        name="aia-dv360-to-bigquery",
        task_id="aia-dv360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dv360(),
        
       
    )
    kube_cm360 = KubernetesPodOperator(
        name="aia-cm360-to-bigquery",
        task_id="aia-cm360_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_cm360(),

       
    )
    
    kube_ttd = KubernetesPodOperator(
        name="aia-ttd-to-bigquery",
        task_id="aia-ttd_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_ttd(),
        
        
    )

    env = get_meltano_env()
    search_list = {}
    google_ads_search_list = ['brand_search_raw','dt']
    key_list = ['marketing','brand']
    for label in google_ads_search_list:
        if label == 'dt':
            env['DBT_BIGQUERY_DATASET'] = 'google_ads_search_transformed__marketing'
            key = 'marketing'
        else:
            env['DBT_BIGQUERY_DATASET'] = 'google_ads_search_transformed__brand' 
            key = 'brand'
        kube_google_ads_search = KubernetesPodOperator(
                name=f"aia-google-ads-search-to-bigquery-{key}",
                task_id=f"aia-google_ads_search_to_bigquery-{key}",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", f"google_ads_search_{key}"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_google_ads_search(key),
                
                
            )
        search_list.setdefault(key,[]).append(kube_google_ads_search)
    kube_dash_search = KubernetesPodOperator(
        name="aia-dash-search-to-bigquery",
        task_id="aia-dash_search_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke", "dbt-bigquery", "run", "--select", "dash_table_search"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_dash_search(),
        
        
    )

    kube_dash = KubernetesPodOperator(
        name="aia-dash-to-bigquery",
        task_id="aia-dash_to_bigquery",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select","dash_table"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        trigger_rule = 'all_done',
        env_vars=set_env_vars_dash(),
        
        
        )
    for index in key_list:
        for task in search_list.get(index,[]):
            task >> kube_dash_search

    set_env_task_tiktok >> kube_tiktok
    set_env_task_facebook >> kube_facebook
    set_env_task_dv360 >> kube_dv360
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd 
    set_env_task_linkedin >> kube_linkedin_ads
    set_env_task_outbrain >> kube_outbrain
    kube_google_ads_search >> kube_dash_search
    [kube_tiktok,kube_facebook,kube_dv360,kube_cm360,kube_ttd,kube_outbrain,kube_linkedin_ads] >> kube_dash
    kube_dash >> kube_dash_search >> kube_dash_union
    