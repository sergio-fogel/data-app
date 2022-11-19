from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from modules.extract import _get_stock_data
from modules.load import _insert_daily_data


STOCKS = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


default_args = {"owner": "sergio", "retries": 0, "start_date": datetime(2022, 11, 17)}
with DAG("stocks", default_args=default_args, schedule_interval="0 4 * * *") as dag:

    create_tables = BashOperator(
        task_id="create_tables",
        bash_command='python /opt/airflow/dags/modules/create_tables.py',
    )

    get_data_task = {}
    for company, symbol in STOCKS.items():
        get_data_task[company] = PythonOperator(
            task_id=f'get_daily_data_{company}',
            python_callable=_get_stock_data,
            op_args=[symbol],
        )

    insert_daily_data = PythonOperator(
        task_id="insert_daily_data",
        python_callable=_insert_daily_data
    )

    for company in STOCKS:
        upstream_task = create_tables
        task = get_data_task[company]
        upstream_task.set_downstream(task)
        task.set_downstream(insert_daily_data)
#    insert_daily_data.set_downstream(do_daily_report)

