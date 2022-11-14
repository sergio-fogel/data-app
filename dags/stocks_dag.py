from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from modules.postgres_cli import PostgresClient


default_args = {"owner": "sergio", "retries": 0, "start_date": datetime(2022, 11, 10)}
with DAG("stocks", default_args=default_args, schedule_interval="0 4 * * *") as dag:

    create_tables = BashOperator(
        task_id="create_tables",
        bash_command='python /opt/airflow/dags/modules/create_tables.py',
    )

    get_daily_data = BashOperator(
        task_id="get_daily_data",
        bash_command='python /opt/airflow/dags/modules/extract.py',
    )

#    # Add insert stock data
#    insert_daily_data = PythonOperator(
#        task_id="insert_daily_data", python_callable=_insert_daily_data
#    )
    create_tables >> get_daily_data

