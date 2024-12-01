from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from scripts.utils.utils import read_config
from scripts.fx_300cp.ingestion import upload_to_minio_parquet

CONFIG = read_config()

S3_RAW_BUCKET = CONFIG['minio']['raw_bucket']

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="fx_rates_300p",
    default_args=default_args,
    # schedule_interval=timedelta(minutes=1),  # Run every minute
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:

    ingestion_upload_to_s3 = PythonOperator(
        task_id="ingestion_upload_to_s3",
        python_callable=upload_to_minio_parquet,
        provide_context=True,
        op_kwargs={"bucket_name": S3_RAW_BUCKET},
    )

    # Define the SparkSubmit task
    spark_submit_task = SparkSubmitOperator(
        task_id="spark_submit_task",
        application="scripts/fx_300cp/spark_processing.py",
        conn_id="spark_conn",
        application_args=[
            "{{ task_instance.xcom_pull(task_ids='ingestion_upload_to_s3') }}"  # Pass the file name
        ],
    )

    run_streamlit_task = BashOperator(
        task_id="run_streamlit_task",
        bash_command=(
            "streamlit run /opt/airflow/scripts/streamlit/app.py "
            "--server.address=0.0.0.0 --server.port=8501"
        ),
    )

    # Define the task pipeline
    ingestion_upload_to_s3 >> spark_submit_task >> run_streamlit_task
