import datetime
from airflow import models
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
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
from comparison_package import ComparisonTrigger
from datetime import timedelta,datetime, timezone
import datetime
from google.cloud import secretmanager
from google.cloud import storage 
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import storage
import json


IMAGE = "australia-southeast1-docker.pkg.dev/amp-main/meltano/meltano-amp-main:prod"


log: logging.log = logging.getLogger("airflow.task")
log.setLevel(logging.INFO)
all_tasks = []
per_label_task = {}
local_tz = pendulum.timezone("Pacific/Auckland")
comparison_start_date = (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
default_args = {
    "retries": 3,
    "max_active_runs": 1,
    "concurrency": 1,
    "catchup": False,
    'retry_delay': datetime.timedelta(minutes=5),
    "start_date": datetime.datetime(2025, 1, 1, tzinfo=local_tz)
}

def get_meltano_env():
    # Update meltano_env with dynamic dates
    meltano_env_unique = Variable.get("meltano_amp_main", deserialize_json=True)
    meltano_env_common = Variable.get("meltano_common_secret",deserialize_json=True)
    meltano_env = {**meltano_env_common, **meltano_env_unique}

    yesterday = datetime.datetime.now(local_tz) - datetime.timedelta(days=7)
    start_date_str = yesterday.strftime("%Y-%m-%d")
    

    meltano_env["START_DATE"] = start_date_str
    meltano_env["BQ_METHOD"] = "batch_job"

    return deepcopy(meltano_env)
def get_ga4_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
def get_ttd_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
def get_facebook_start_date():
    return (datetime.datetime.now(local_tz) - datetime.timedelta(days=2)).strftime("%Y-%m-%dT00:00:00Z")
with models.DAG(
    dag_id="amp-meltano-google-ads",
    schedule_interval = "0 14 * * *",
    default_args=default_args,
) as dag_google_ads:
    def set_env_vars_dash_union(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env
    def set_env_vars_google_ads_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'google_ads_search_transformed'
        return env
    set_env_task_google_ads_search = PythonOperator(
        task_id="set_env_task_google_ads_search",
        python_callable=set_env_vars_google_ads_search,
    )
    set_env_task_dash = PythonOperator(
        task_id="set_env_task_dash",
        python_callable=set_env_vars_dash,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_task_dash_search",
        python_callable=set_env_vars_dash_search,
    )
    
    kube_dash = KubernetesPodOperator(
                name="amp-dash-to-bigquery",
                task_id="amp-dash_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke","dbt-bigquery:dash_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_dash(),
                trigger_rule="all_done",
                #base_container_name=f"meltano-{label}-dash",
                get_logs = True
        )
    kube_dash_search = KubernetesPodOperator(
                name="amp-dash-search-to-bigquery",
                task_id="amp-dash-search_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod","invoke","dbt-bigquery:dash_search_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_dash_search(),
                #base_container_name=f"meltano-{label}-dash-search",
                get_logs = True
        )
    kube_google_ads_search = KubernetesPodOperator(
            name="amp-google-ads-search-to-bigquery",
            task_id="amp-google-ads-search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:google_ads_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_google_ads_search(),
            #base_container_name=f"meltano-{label}-google-ads-search",
            get_logs = True
    )
    set_env_task_dash >> kube_dash
    set_env_task_dash_search >> kube_dash_search
    set_env_task_google_ads_search >> kube_google_ads_search
    kube_google_ads_search >> kube_dash
    brands = ['centralized','wealth','general_insurance']
    #task_list = [kube_cm360,kube_ttd,kube_linkedin,kube_hivestack,kube_facebook,kube_reddit] 
    for brand in brands:
        if brand == 'centralized':
            kube_dash_union_centralized = KubernetesPodOperator(
                    name=f"amp-dash-union-{brand}-to-bigquery",
                    task_id=f"amp-dash_union_{brand}_to_bigquery",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_dash(),
                    #base_container_name=f"meltano-{label}-dash",
                    get_logs = True
            )
            kube_dash_search >> kube_dash_union_centralized
        else:
  

            kube_dash_union = KubernetesPodOperator(
                    name=f"amp-dash-union-{brand}-to-bigquery",
                    task_id=f"amp-dash_union_{brand}_to_bigquery",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_dash_union(brand),
                    #base_container_name=f"meltano-{label}-dash",
                    get_logs = True
            )
            kube_dash_union_centralized >> kube_dash_union
    set_env_task_dash >> kube_dash >> kube_dash_search
    


with models.DAG(
    dag_id="amp-meltano-extraction-transformation-dbt",
    schedule_interval="30 6 * * *"
,
    default_args=default_args,
) as dag:
    env = get_meltano_env()

        
    def set_env_vars_hivestack():

        env = get_meltano_env()
        env["BQ_DATASET"] = "hivestack_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'hivestack_transformed'
        return env   
    def set_env_vars_adobe_centralized():
        env=get_meltano_env()
        env["BQ_DATASET"] = "adobe_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'adobe_transformed'
        return env   
    def set_env_vars_adobe(label):
        env=get_meltano_env()
        env["BQ_DATASET"] = "adobe_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = f'ga4_transformed__{label}'
        return env
    def set_env_vars_facebook():
        env = get_meltano_env()
        env["BQ_DATASET"] = "facebook_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'facebook_transformed'
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"] = get_facebook_start_date()
        return env
    def set_env_vars_cm360():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'cm360_transformed'
        return env

    def set_env_vars_ttd():
        env = get_meltano_env()
        env["BQ_DATASET"] = "ttd_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'ttd_transformed'
        env["TAP_TTD_START_DATE"] = get_ttd_start_date()
        #env["TAP_TTD_ADVERTISER_ID"] = id
        return env
    def set_env_vars_reddit():
        env = get_meltano_env()
        env["BQ_DATASET"] = "reddit_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'reddit_transformed'
        #env["TAP_TTD_ADVERTISER_ID"] = id
        return env
    def set_env_vars_dash_union(label):
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = f'dash_table__{label}'
        return env
    def set_env_vars_linkedin():
        env = get_meltano_env()
        env["BQ_DATASET"] = "linkedin_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'linkedin_transformed'
        return env
    def set_env_vars_dv360():
        env = get_meltano_env()
        env["BQ_DATASET"] = "dv360_raw"
        env["BQ_METHOD"] = "batch_job"
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'dv360_transformed'
        return env
    def set_env_vars_dash():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table'
        return env
    def set_env_vars_dash_search():
        env = get_meltano_env()
        env["DBT_BIGQUERY_METHOD"] = 'oauth'
        env["DBT_BIGQUERY_PROJECT"] = 'amp-main'
        env["DBT_BIGQUERY_DATASET"] = 'dash_table_search'
        return env

    
    set_env_task_facebook = PythonOperator(
        task_id="set_env_task_facebook",
        python_callable=set_env_vars_facebook,
    )
    set_env_task_hivestack = PythonOperator(
        task_id="set_env_task_hivestack",
        python_callable=set_env_vars_hivestack,
    )
    set_env_task_cm360 = PythonOperator(
        task_id="set_env_task_cm360",
        python_callable=set_env_vars_cm360,
    )
    set_env_task_ttd = PythonOperator(
        task_id="set_env_task_ttd",
        python_callable=set_env_vars_ttd,
    )
    set_env_task_reddit = PythonOperator(
        task_id="set_env_task_reddit",
        python_callable=set_env_vars_reddit,
    )

    set_env_task_adobe_centralized = PythonOperator(
        task_id="set_env_task_adobe_centralized",
        python_callable=set_env_vars_adobe_centralized,
    )
    set_env_task_linkedin = PythonOperator(
        task_id="set_env_task_linkedin",
        python_callable=set_env_vars_linkedin,
    )
    set_env_task_dash = PythonOperator(
        task_id="set_env_task_dash",
        python_callable=set_env_vars_dash,
    )
    set_env_task_dash_search = PythonOperator(
        task_id="set_env_task_dash_search",
        python_callable=set_env_vars_dash_search,
    )
    set_env_task_dv360 = PythonOperator(
        task_id="set_env_task_dv360",
        python_callable=set_env_vars_dv360,
    )
    comparison_trigger_facebook = ComparisonTrigger(
        project_name="amp-main",
        destination_table="facebook_transformed",
        table_name="facebook",
        source_name="meta",
        start_date=comparison_start_date,
        end_date=datetime.datetime.now(local_tz).strftime("%Y-%m-%d"),
        secret_name="airflow-variables-meltano_amp_main",
        project_id=env["PROJECT_ID"]
        )

    
    task_facebook_comparison = PythonOperator(
        task_id="task_facebook_comparison",
        python_callable=comparison_trigger_facebook.compare_data,
        trigger_rule="all_done",
    )
  
    kube_adobe_centralized = KubernetesPodOperator(
            name="amp-adobe-centralized-to-bigquery",
            task_id="amp-adobe_centralized_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery:adobe_centralized_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_adobe_centralized(),
            get_logs = True
        )
    
    kube_reddit = KubernetesPodOperator(
            name="amp-reddit-to-bigquery",
            task_id="amp-reddit_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-reddit-ads", "target-bigquery","dbt-bigquery:reddit_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_reddit(),
            #base_container_name=f"meltano-{label}-reddit",
            get_logs = True
    )
    kube_dv360 = KubernetesPodOperator(
            name="amp-dv360-to-bigquery",
            task_id="amp-dv360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-dv360", "target-bigquery","dbt-bigquery:dv360_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dv360(),
            #base_container_name=f"meltano-{label}-dv360",
            get_logs = True
        )
   
    kube_hivestack = KubernetesPodOperator(
            name="amp-hivestack-to-bigquery",
            task_id="amp-hivestack_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-hivestack", "target-bigquery",f"dbt-bigquery:hivestack_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_hivestack(),
            #base_container_name=f"meltano-{label}-hivestack",
            get_logs = True
        )
    
    kube_linkedin = KubernetesPodOperator(
            name="amp-linkedin-to-bigquery",
            task_id="amp-linkedin_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run","tap-linkedin-ads","target-bigquery","dbt-bigquery:linkedin_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_linkedin(),
            #base_container_name=f"meltano-{label}-linkedin",
            get_logs = True
        )
    kube_facebook = KubernetesPodOperator(
            name="amp-facebook-to-bigquery",
            task_id="amp-facebook_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_facebook(),
            #base_container_name=f"meltano-{label}-facebook",
            get_logs = True
    )
    def set_env_vars_facebook_retry():
        env = set_env_vars_facebook()
        env["TAP_FACEBOOK_AIRBYTE_CONFIG_START_DATE"] = comparison_start_date + "T00:00:00Z"
        return env
    kube_facebook_retry = KubernetesPodOperator(
        name="amp-facebook-retry",
        task_id="amp-facebook_retry",
        namespace="composer-user-workloads",
        image=IMAGE,
        arguments=["--environment=prod", "run", "tap-facebook", "target-bigquery","dbt-bigquery:facebook_models"],
        container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "1000M", "cpu": "500m"},
        ),
        env_vars=set_env_vars_facebook_retry(),
        #base_container_name=f"meltano-{label}-facebook",
        get_logs = True
    )
    kube_facebook_done = EmptyOperator(
        task_id="kube_facebook_done",
        trigger_rule="all_done",
    )
    kube_facebook_union = EmptyOperator(
        task_id="kube_facebook_union",
        trigger_rule="all_done",
    )
    kube_facebook_done >> kube_facebook_union
    kube_facebook_retry >> kube_facebook_union
    # increase retry delay, reduce retries to 1 for ttd
    kube_ttd = KubernetesPodOperator(
            name="amp-ttd-to-bigquery",
            task_id="amp-ttd_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "run", "tap-ttd", "target-bigquery","dbt-bigquery:ttd_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_ttd(),
            execution_timeout=timedelta(minutes=60),
            retries=1,
            retry_delay=timedelta(minutes=20),
            #base_container_name=f"meltano-{label}-ttd",
            get_logs = True
    )
    kube_cm360 = KubernetesPodOperator(
            name="amp-cm360-to-bigquery",
            task_id="amp-cm360_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke", "dbt-bigquery:cm360_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_cm360(),
            #base_container_name=f"meltano-{label}-cm360",
            get_logs = True
    )

    kube_dash = KubernetesPodOperator(
            name="amp-dash-to-bigquery",
            task_id="amp-dash_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod", "invoke","dbt-bigquery:dash_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash(),
            trigger_rule="all_done",
            #base_container_name=f"meltano-{label}-dash",
            get_logs = True
    )
    kube_dash_search = KubernetesPodOperator(
            name="amp-dash-search-to-bigquery",
            task_id="amp-dash-search_to_bigquery",
            namespace="composer-user-workloads",
            image=IMAGE,
            arguments=["--environment=prod","invoke","dbt-bigquery:dash_search_models"],
            container_resources=k8s_models.V1ResourceRequirements(
                limits={"memory": "1000M", "cpu": "500m"},
            ),
            env_vars=set_env_vars_dash_search(),
            #base_container_name=f"meltano-{label}-dash-search",
            get_logs = True
    )
    set_env_task_adobe_centralized >> kube_adobe_centralized
    set_env_task_cm360 >> kube_cm360 >> set_env_task_ttd >> kube_ttd
    set_env_task_linkedin >> kube_linkedin
    set_env_task_reddit >> kube_reddit
    set_env_task_hivestack >> kube_hivestack
    set_env_task_facebook >> kube_facebook
    set_env_task_dv360 >> kube_dv360
    @task.branch(task_id="branch_task_facebook_comparison")
    def branc_task_facebook_comparison(**context):
        xcom_value = context['ti'].xcom_pull(task_ids="task_facebook_comparison")
        if not xcom_value:
            return 'amp-facebook_retry'
        return 'kube_facebook_done'

    set_env_task_facebook >> kube_facebook >> task_facebook_comparison >> branc_task_facebook_comparison() >> [kube_facebook_done, kube_facebook_retry]
    set_env_task_dash_search >> kube_dash_search
    kube_dash >> kube_dash_search
        
    brands = ['centralized','wealth','general_insurance','direct']
    task_list = [kube_cm360,kube_ttd,kube_dv360,kube_linkedin,kube_hivestack,kube_facebook_union,kube_reddit] 
    for brand in brands:
        if brand == 'centralized':
            kube_dash_union_centralized = KubernetesPodOperator(
                    name=f"amp-dash-union-{brand}-to-bigquery",
                    task_id=f"amp-dash_union_{brand}_to_bigquery",
                    namespace="composer-user-workloads",
                    image=IMAGE,
                    arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{brand}"],
                    container_resources=k8s_models.V1ResourceRequirements(
                        limits={"memory": "1000M", "cpu": "500m"},
                    ),
                    env_vars=set_env_vars_dash(),
                    #base_container_name=f"meltano-{label}-dash",
                    get_logs = True
            )
            kube_dash_search >> kube_dash_union_centralized
        else:
            if brand != 'direct':
                kube_adobe = KubernetesPodOperator(
                name=f"amp-adobe-{brand}-to-bigquery",
                task_id=f"amp-adobe_{brand}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke",f"dbt-bigquery:adobe_{brand}_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_adobe(brand),
                #base_container_name=f"meltano-{label}-adobe",
                get_logs = True
                )  
                kube_adobe_centralized >> kube_adobe 
                task_list.append(kube_adobe)
            
                kube_dash_union = KubernetesPodOperator(
                        name=f"amp-dash-union-{brand}-to-bigquery",
                        task_id=f"amp-dash_union_{brand}_to_bigquery",
                        namespace="composer-user-workloads",
                        image=IMAGE,
                        arguments=["--environment=prod", "invoke","dbt-bigquery","run","--select",f"dash_union__{brand}"],
                        container_resources=k8s_models.V1ResourceRequirements(
                            limits={"memory": "1000M", "cpu": "500m"},
                        ),
                        env_vars=set_env_vars_dash_union(brand),
                        #base_container_name=f"meltano-{label}-dash",
                        get_logs = True
                )
                kube_dash_union_centralized >> kube_dash_union
            else:
                kube_adobe = KubernetesPodOperator(
                name=f"amp-adobe-{brand}-to-bigquery",
                task_id=f"amp-adobe_{brand}_to_bigquery",
                namespace="composer-user-workloads",
                image=IMAGE,
                arguments=["--environment=prod", "invoke",f"dbt-bigquery:adobe_{brand}_models"],
                container_resources=k8s_models.V1ResourceRequirements(
                    limits={"memory": "1000M", "cpu": "500m"},
                ),
                env_vars=set_env_vars_adobe(brand),
                #base_container_name=f"meltano-{label}-adobe",
                get_logs = True
                )  
                kube_adobe_centralized >> kube_adobe 
                task_list.append(kube_adobe)
    task_list >> set_env_task_dash >> kube_dash
            
