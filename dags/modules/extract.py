import requests
from time import sleep

from modules.transform import _transform_stock_data


BASE_URL = "https://www.alphavantage.co/query"
STOCK_FN = "TIME_SERIES_DAILY_ADJUSTED"
API_KEY = "BXG1EVYYG12UVSSQ"
#https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=GOOG&apikey=BXG1EVYYG12UVSSQ


def _extract_stock_data(stock_symbol, date):
    end_point = (f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}&apikey={API_KEY}&datatype=json")
    r = requests.get(end_point)
    sleep(61)
    resp = r.content
    data = _transform_stock_data(resp, date, stock_symbol)
    return data.to_json()


#    if __name__ == "__main__":

