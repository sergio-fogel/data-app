import json
import pandas as pd
import numpy as np


def _transform_stock_data(resp, date, stock_symbol):
    data = json.loads(resp)
    df = (
        pd.DataFrame(data['Time Series (Daily)'])
        .T.reset_index()   #¿¿¿.T??? --> pasa fechas de columnas a filas | reset_index():
        .rename(columns={'index': 'date', '1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '6. volume': 'volume'})
    )
    df = df[df['date'] == date] #FILTRO: seleccionar sólo los registros correspondientes a la execution date
    if not df.empty:
        for c in df.columns:
            if c != 'date':
                df[c] = df[c].astype(float) #CASTEO de datos
    else:
        df = pd.DataFrame(
            [[date, np.nan, np.nan, np.nan, np.nan, np.nan]], columns=['date', 'open', 'high', 'low', 'close', 'volume']
        )
    df['symbol'] = stock_symbol
    df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
    return(df)


#if __name__ == "__main__":

