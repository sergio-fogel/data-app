import requests
import json
import pandas as pd


BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "BXG1EVYYG12UVSSQ"
STOCK_FN = "TIME_SERIES_DAILY_ADJUSTED"
#https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=GOOG&apikey=BXG1EVYYG12UVSSQ


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

