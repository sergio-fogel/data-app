import pandas as pd
import sqlalchemy.exc

from modules.postgres_cli import PostgresClient


def _insert_daily_data(date):

    db = 'stocks'
    postgres_cli = PostgresClient(db)

    with open(f'/opt/airflow/raw_data/{date}/{date}.json', 'r') as f:
        insert_df = pd.read_json(
            f,
            orient='index',
            ).T
        insert_df = insert_df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]

    try:
        postgres_cli.insert_from_frame(insert_df, 'stock_value')
        print(f"Inserted {len(insert_df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")

