import json
from datetime import datetime

import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from postgres_cli import PostgresClient


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "BXG1EVYYG12UVSSQ"
STOCK_FN = "TIME_SERIES_DAILY"


default_args = {"owner": "sergio", "retries": 0, "start_date": datetime(2022, 10, 30)}
with DAG("stocks", default_args=default_args, schedule_interval="0 4 * * *") as dag:
    create_migration = BashOperator(
        task_id="create_migration",
        bash_command='python -m alembic -c /tmp/models/alembic.ini revision --autogenerate -m "Create stock value data model"',
    )
    run_migration = BashOperator(
        task_id="run_migration",
        bash_command='python -m alembic upgrade heads',
    )

#    get_daily_data = PythonOperator(
#        task_id="get_daily_data", python_callable=_get_stock_data, op_args=["aapl"]
#    )
#    # Add insert stock data
#    insert_daily_data = PythonOperator(
#        task_id="insert_daily_data", python_callable=_insert_daily_data
#    )
    create_migration >> run_migration