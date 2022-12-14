########## TEST 1 (EXTRACT) ##########

#Debería validar la lógica de extracción

#Para que el test no vaya en serio a hacer el request a la API, mockear la entrada (hacer un mock del get que devuelva una data hardcodeada y status 200)
#Y lo mismo para el lado de la escritura (return df to JSON), así el test no escribe nada realmente en ningún file
#Y validar que todo el proceso del medio corra OK


import requests
import requests_mock
import pandas as pd

from dags.modules.transform import _transform_stock_data


def test_extract():

    expected_data = pd.DataFrame([['GOOG','2022-12-12',93.09,93.8745,91.9,93.56,27380948.0]], columns=['symbol','date','open','high','low','close','volume'])

    #expected_data = {"symbol":{"1":"GOOG"},"date":{"1":"2022-12-12"},"open":{"1":93.09},"high":{"1":93.8745},"low":{"1":91.9},"close":{"1":93.56},"volume":{"1":27380948.0}}

    adapter = requests_mock.Adapter()
    session = requests.Session()
    session.mount('mock://', adapter)

    adapter.register_uri(
        'GET',
        'mock://test.com/1',
        json={
        "Meta Data": {
            "1. Information": "Daily Time Series with Splits and Dividend Events",
            "2. Symbol": "GOOG",
            "3. Last Refreshed": "2022-12-13",
            "4. Output Size": "Compact",
            "5. Time Zone": "US/Eastern"
        },
        "Time Series (Daily)": {
            "2022-12-12": {
                "1. open": "93.09",
                "2. high": "93.8745",
                "3. low": "91.9",
                "4. close": "93.56",
                "5. adjusted close": "93.56",
                "6. volume": "27380948",
                "7. dividend amount": "0.0000",
                "8. split coefficient": "1.0"
            }}},
        status_code=200
        )

    r = session.get('mock://test.com/1')
    resp = r.content

    data = _transform_stock_data(resp, '2022-12-12', 'GOOG')

    #assert data == expected_data
    pd.testing.assert_frame_equal(data,expected_data)

