import requests
from time import sleep
import json
import pandas as pd
import numpy as np


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "BXG1EVYYG12UVSSQ"
STOCK_FN = "TIME_SERIES_DAILY_ADJUSTED"
#https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=GOOG&apikey=BXG1EVYYG12UVSSQ


def _get_stock_data(stock_symbol, date):

    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")

    r = requests.get(end_point)

    sleep(61)

    data = json.loads(r.content)

    df = (
        pd.DataFrame(data['Time Series (Daily)'])
        .T.reset_index()   #¿¿¿.T??? --> pasa fechas de columnas a filas | reset_index(): indice con valores duplicados
        .rename(columns={'index': 'Date', '1. open': 'Open', '2. high': 'High', '3. low': 'Low', '4. close': 'Close', '6. volume': 'Volume'})
    )

    df = df[df['Date'] == date] #FILTRO: seleccionar sólo los registros correspondientes a la execution date

    if not df.empty:
        for c in df.columns:
            if c != 'Date':
                df[c] = df[c].astype(float)
    else:
        df = pd.DataFrame(
            [[date, np.nan, np.nan, np.nan, np.nan, np.nan]], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        )

    df['Symbol'] = stock_symbol

    df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    return df.to_json()


if __name__ == "__main__":
    _get_stock_data('GOOG', '2022-11-25')

