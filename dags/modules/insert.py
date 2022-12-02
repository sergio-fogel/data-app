from datetime import datetime
import pandas as pd
import sqlalchemy.exc

from modules.postgres_cli import PostgresClient


def _insert_daily_data(date):

    db = 'stocks'
    postgres_cli = PostgresClient(db)
    connection = postgres_cli._connect()

    with open(f'/opt/airflow/raw_data/{date}/{date}.json', 'r') as f:
        insert_df = pd.read_json(
            f,
            orient='index',
            ).T
        insert_df = insert_df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    try:
        postgres_cli.insert_from_frame(insert_df, 'stock_value')
#        insert_df.to_sql('stock_value', con=connection, if_exists='append', index=False)
        print(f"Inserted {len(insert_df)} records")
    except sqlalchemy.exc.IntegrityError:
        # You can avoid doing this by setting a trigger rule in the reports operator
        print("Data already exists! Nothing to do...")

