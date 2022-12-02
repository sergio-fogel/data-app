import psycopg2
from psycopg2 import sql
import mplfinance as mpf
from pathlib import Path
import pandas as pd

from modules.postgres_cli import PostgresClient

#https://github.com/matplotlib/mplfinance/blob/master/src/mplfinance/_arg_validators.py


def _stockv_weekly_report(symbol, date):

    con = psycopg2.connect(dbname='stocks', user='airflow', password='airflow', host='postgres', port='5432')
    cur = con.cursor()
    
    query = sql.SQL('select {fields} from {table} where {pkey} = %s').format(
        fields=sql.SQL(',').join([
            sql.Identifier('Symbol'),
            sql.Identifier('Date'),
            sql.Identifier('Open'),
            sql.Identifier('High'),
            sql.Identifier('Low'),
            sql.Identifier('Close'),
            sql.Identifier('Volume'),
        ]),
        table=sql.Identifier('stock_value'),
        pkey=sql.Identifier('Symbol'))
    
    cur.execute(query, (symbol,))
    rows = cur.fetchall()
    df_report = pd.DataFrame(rows, columns=[x[0] for x in cur.description])

#    filepath = Path(f'/opt/airflow/reports/{date}/{symbol}.json')
#    filepath.parent.mkdir(parents=True, exist_ok=True)
#    return df_report.to_json(filepath)

    format = '%Y-%m-%d %H:%M:%S'
    df_report['Datetime'] = pd.to_datetime(df_report['Date'], format=format)
    df_report = df_report.set_index(pd.DatetimeIndex(df_report['Datetime']))

    filepath = Path(f'/opt/airflow/reports/{date}/{symbol}.png')
    filepath.parent.mkdir(parents=True, exist_ok=True)
        
    mpf.plot(df_report, savefig=filepath)


if __name__ == '__main__':
    _stockv_weekly_report()

