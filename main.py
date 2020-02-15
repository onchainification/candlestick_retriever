import time
import requests
from datetime import datetime

import pandas as pd

def get_batch(symbol, interval, start_time=0, limit=1000):
    """get as many candlesticks as possible in one go"""

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'limit': limit
    }
    try:
        response = requests.get('https://api.binance.com/api/v3/klines', params)
    except requests.exceptions.ConnectionError:
        print('Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    return pd.DataFrame(response.json())


def all_candles_to_csv(base='BTC', quote='USDT', interval='1m'):
    """fill a dataframe with all candlesticks of a trading pair since its inception"""

    # see if there is any data saved already
    try:
        df = pd.read_csv(f'data/{base}-{quote}.csv')

        # column labels from the csv come back as strings vs ints of api response
        df.columns = df.columns.astype(int)

        last_timestamp = df.iloc[-1, 0]
    except FileNotFoundError:
        last_timestamp = 0

    last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

    batches = []

    # gather all batches available, starting from the last timestamp saved
    # stop if candlesticks with date of today are reached
    while last_datetime.strftime('%Y%m%d') < datetime.now().strftime('%Y%m%d'):
        batches.append(get_batch(symbol=base+quote, interval=interval, start_time=last_timestamp))

        last_timestamp = batches[-1].iloc[-1, 0]
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

        print(base, quote, interval, last_datetime, end='\r', flush=True)

    if len(batches) > 0:
        pd.concat(batches, ignore_index=True).to_csv(f'data/{base}-{quote}.csv', index=False)
        return True


for pair in requests.get('https://api.binance.com/api/v3/exchangeInfo').json()['symbols']:
    if pair['quoteAsset'] == 'USDT' and pair['status'] == 'TRADING':
        if all_candles_to_csv(base=pair['baseAsset'], quote=pair['quoteAsset']) is True:
            print(f'Wrote new candles to file for {pair["symbol"]}')
        else:
            print(f'Already up to date with {pair["symbol"]}')
