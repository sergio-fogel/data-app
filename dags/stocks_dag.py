from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

import json
from pathlib import Path

import pandas as pd
import requests
import sqlalchemy.exc
from modules.postgres_cli import PostgresClient


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "BXG1EVYYG12UVSSQ"
STOCK_FN = "TIME_SERIES_DAILY_ADJUSTED"
#https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=GOOG&apikey=BXG1EVYYG12UVSSQ

STOCKS = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


def _get_stock_data(stock_symbol, **context):

#    date = f"{date:%Y-%m-%d}"  # read execution date from context

    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")

    r = requests.get(end_point)

    data = json.loads(r.content)

    df = (
        pd.DataFrame(data['Time Series (Daily)'])
        .T.reset_index()   #¿¿¿.T??? --> pasa fechas de columnas a filas
        .rename(columns={'index': 'date', '1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. adjusted close': 'adjusted_close', '6. volume': 'volume', '7. dividend amount': 'dividend_amount', '8. split coefficient': 'split_coefficient'})
    )

#    df = df[df['date'] == date]

#    df['date'] = date

    df['symbol'] = stock_symbol

    return df.to_json()


def _insert_daily_data(**context):

    task_instance = context['ti'] # Task instances store the state of a task instance.
    
    # Get xcom for each upstream task
    dfs = []
#    for ticker in STOCKS:
    for company, symbol in STOCKS.items():
        stock_df = pd.read_json(
            task_instance.xcom_pull(task_ids=f'get_daily_data_{company}'),
            orient='index',
        ).T
        dfs.append(stock_df)

    df = pd.concat(dfs, axis=0)

    filepath = Path(f'/opt/airflow/raw_data/{datetime.today().strftime("%y-%m-%d")}/{datetime.today().strftime("%y-%m-%d")}.csv')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath)

    db = 'stocks'
    postgres_cli = PostgresClient(db)
#    sql_cli = PostgresClient('postgresql+psycopg2://airflow:airflow@postgres/stocks')

    try:
        postgres_cli.insert_from_frame(df, 'stock_value')
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")


default_args = {"owner": "sergio", "retries": 0, "start_date": datetime(2022, 11, 15)}
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

