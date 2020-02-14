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
    response = requests.get('https://api.binance.com/api/v3/klines', params)
    return response.json()


def get_all_candles(symbol='BTCUSDT', interval='1m'):
    """fill a dataframe with all candlesticks of a trading pair since its inception"""

    df = pd.DataFrame.from_dict(get_batch(symbol=symbol, interval=interval, limit=1))
    while True:
        last_timestamp = df.iloc[-1, 0]
        last_datetime = datetime.fromtimestamp(last_timestamp / 1000)
        df = df.append(
            get_batch(symbol=symbol, interval=interval, start_time=last_timestamp),
            ignore_index=True)
        print(symbol, interval, last_datetime)
        if last_datetime.strftime('%Y%m%d') >= datetime.now().strftime('%Y%m%d'):
            return df


def quick_clean(df):
    """add headers and convert all columns to their proper dtype"""

    df.columns = [
        'open_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_asset_volume',
        'number_of_trades',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume',
        'ignore'
    ]

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df = df.set_index('open_time', drop=True)

    df = df.astype(dtype={
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64',
        'close_time': 'datetime64[ms]',
        'quote_asset_volume': 'float64',
        'number_of_trades': 'int64',
        'taker_buy_base_asset_volume': 'float64',
        'taker_buy_quote_asset_volume': 'float64',
        'ignore': 'float64'
    })

    return df


trading_pairs = []
for pair in requests.get('https://api.binance.com/api/v3/exchangeInfo').json()['symbols']:
    trading_pairs.append(pair['symbol'])

trading_pairs = ['ETHUSDT', 'BNBUSDT']

for pair in trading_pairs:
    df = get_all_candles(symbol=pair)
    df = quick_clean(df)
    df.to_csv(f'{pair}.csv')
