from airflow.decorators import dag, task
from datetime import datetime, timezone, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from alerts import send_slack_alert
from scripts.fetch_data_bronze_erro import fetch_and_save_breweries_to_bronze

BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"
GOLD_LAYER_PATH = "/opt/airflow/data_lake/gold"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
    'on_failure_callback': send_slack_alert,
}

@dag(
    dag_id='open_brewery_data_pipeline_erro',
    start_date=datetime(2025, 7, 9, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['brewery', 'data_lake', 'etl'],
    default_args=default_args,
)
def open_brewery_data_pipeline():
    @task
    def fetch_data_to_bronze_task():
        return fetch_and_save_breweries_to_bronze(BRONZE_LAYER_PATH)

    bronze_output_path = fetch_data_to_bronze_task()

    transform_to_silver_task = SparkSubmitOperator(
        task_id='transform_to_silver_task',
        conn_id='spark_default',
        application='/opt/airflow/spark/breweries/jobs/silver.py',
        py_files='/opt/airflow/dags/utils/text_processing.py,/opt/airflow/dags/utils/metadata_utils.py,/opt/airflow/spark/breweries/queries/queries_silver_breweries.py',
        application_args=[bronze_output_path],
        packages='io.delta:delta-spark_2.12:3.0.0',
    )

    transform_to_gold_task = SparkSubmitOperator(
        task_id='transform_to_gold',
        application='/opt/airflow/spark/breweries/jobs/gold.py',
        conn_id='spark_default',
        conf={"spark.driver.maxResultSize": "4g"},
        application_args=[
            '--silver_layer_path', SILVER_LAYER_PATH,
            '--gold_layer_path', GOLD_LAYER_PATH
        ],
        py_files='/opt/airflow/dags/utils/metadata_utils.py,/opt/airflow/spark/breweries/queries/queries_gold_breweries.py',
        packages='io.delta:delta-spark_2.12:3.0.0',
    )

    bronze_output_path >> transform_to_silver_task >> transform_to_gold_task

open_brewery_data_pipeline()