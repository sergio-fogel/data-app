import pandas as pd
from pathlib import Path
from datetime import datetime
import sqlalchemy.exc

from modules.postgres_cli import PostgresClient


STOCKS = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


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

