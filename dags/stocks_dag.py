from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from pathlib import Path

from modules.extract import _get_stock_data
from modules.insert import _insert_daily_data
from modules.reports import _stockv_weekly_report


STOCKS = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


def _load_daily_data(date, **context):
    task_instance = context['ti'] #Task instances store the state of a task instance.
    #Get xcom for each upstream task
    dfs = []

    for ticker in STOCKS:
        stock_df = pd.read_json(
            task_instance.xcom_pull(task_ids=f'get_daily_data_{ticker}'),
            orient='index', #if the keys should be rows, pass ‘index’
            ).T #(.T) Transpose index and columns: Reflect the DataFrame over its main diagonal by writing rows as columns and vice-versa.
        stock_df = stock_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        dfs.append(stock_df)

    df_concat = pd.concat(dfs, axis=0)
    df_concat.reset_index(inplace=True) #permitir indices con valores duplicados

    filepath = Path(f'/opt/airflow/raw_data/{date}/{date}.json')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df_concat.to_json(filepath)


default_args = {"owner": "sergio", "retries": 0, "start_date": datetime(2022, 11, 23)}
with DAG("stocks", default_args=default_args, schedule_interval="0 4 * * *") as dag:

    create_tables = BashOperator(
        task_id="create_tables",
        bash_command='python /opt/airflow/dags/modules/create_tables.py',
    )

    get_data_task = {}
    EXEC_DATE = '{{ ds }}'
    for company, symbol in STOCKS.items():
        get_data_task[company] = PythonOperator(
            task_id=f'get_daily_data_{company}',
            python_callable=_get_stock_data,
            op_args=[symbol, EXEC_DATE],
        )

    load_data_task = {}
    EXEC_DATE = '{{ ds }}'
    load_daily_data = PythonOperator(
        task_id='load_daily_data',
        python_callable=_load_daily_data,
        op_args=[EXEC_DATE]
    )

    insert_data_task = {}
    EXEC_DATE = '{{ ds }}'
    insert_daily_data = PythonOperator(
        task_id='insert_daily_data',
        python_callable=_insert_daily_data,
        op_args=[EXEC_DATE]
    )

    report_task = {}
    EXEC_DATE = '{{ ds }}'
    for company, symbol in STOCKS.items():
        report_task[company] = PythonOperator(
            task_id=f'do_weekly_report_{company}',
            python_callable=_stockv_weekly_report,
            op_args=[symbol, EXEC_DATE]
    )

    for company in STOCKS:
        upstream_task = create_tables
        task = get_data_task[company]
        upstream_task.set_downstream(task)
        task.set_downstream(load_daily_data)
        load_daily_data.set_downstream(insert_daily_data)
        insert_daily_data.set_downstream(report_task[company])

