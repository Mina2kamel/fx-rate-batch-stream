
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.utils.utils import read_sql_file
from scripts.fx_5cp.ingestion import load_data_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fx_rates_5p',
    default_args=default_args,
    description='DAG to calculate current rates and percentage changes for currency pairs',
    schedule_interval='@hourly', # run the dag every hour
    start_date=datetime(2024, 11, 23),
    catchup=False,
) as dag:

    generate_and_load_task = PythonOperator(
        task_id="generate_and_load_data",
        python_callable=load_data_to_postgres,
    )

    query_task = PostgresOperator(
        task_id='calculate_fx_changes',
        postgres_conn_id='postgres_conn',
        sql=read_sql_file('scripts/fx_5cp/fx_rates_5cp.sql'),
    )

    run_streamlit_task = BashOperator(
        task_id="run_streamlit_task",
        bash_command=(
            "streamlit run /opt/airflow/scripts/streamlit/app.py "
            "--server.address=0.0.0.0 --server.port=8501"
        ),
    )

    generate_and_load_task >> query_task >> run_streamlit_task