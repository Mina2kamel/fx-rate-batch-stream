from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from scripts.streaming.kafka_producer import stream_fx_rates

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    'stream_fx_rates',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or adjust as needed
    start_date=datetime(2024, 11, 17),
    catchup=False,
) as dag:

    # Task to stream FX rates
    stream_fx_rates_task = PythonOperator(
        task_id='stream_fx_rates',
        python_callable=stream_fx_rates,
    )

    spark_submit_task = BashOperator(
        task_id="spark_streaming",
        bash_command=(
            "spark-submit --packages org.postgresql:postgresql:42.7.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 /opt/airflow/scripts/streaming/spark_stream.py"
        ),
    )

    stream_fx_rates_task
    spark_submit_task
