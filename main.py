import os
import time
import json
import random
from datetime import date, datetime, timedelta

import requests
import pandas as pd

import preprocessing

API_BASE = 'https://api.binance.com/api/v3/'

LABELS = [
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

def get_batch(symbol, interval, start_time=0, limit=1000):
    """get as many candlesticks as possible in one go"""

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'limit': limit
    }
    try:
        response = requests.get(f'{API_BASE}klines', params)
    except requests.exceptions.ConnectionError:
        print('Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    if response.status_code == 200:
        return pd.DataFrame(response.json(), columns=LABELS)
    print(f'Got erroneous response back: {response}')
    return None


def all_candles_to_csv(base='BTC', quote='USDT', interval='1m'):
    """
    collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to csv
    """

    # see if there is any data saved on disk already
    try:
        batches = [pd.read_csv(f'data/{base}-{quote}.csv')]
        last_timestamp = batches[-1]['open_time'].max()
    except FileNotFoundError:
        batches = [pd.DataFrame([], columns=LABELS)]
        last_timestamp = 0
    old_lines = len(batches[-1].index)

    # gather all candlesticks available, starting from the last timestamp loaded from disk or 0
    # stop if the timestamp that comes back from the api is the same as the last one
    previous_timestamp = None
    if date.fromtimestamp(last_timestamp / 1000) < date.today():
        while previous_timestamp != last_timestamp:
            previous_timestamp = last_timestamp

            new_batch = get_batch(
                symbol=base+quote,
                interval=interval,
                start_time=last_timestamp
            )

            last_timestamp = new_batch['open_time'].max()

            # sometimes no new trades took place yet on date.today();
            # in this case the batch is nothing new
            if previous_timestamp == last_timestamp:
                break

            batches.append(new_batch)

            last_datetime = datetime.fromtimestamp(last_timestamp / 1000)

            covering_spaces = 20 * ' '
            print(datetime.now(), base, quote, interval, str(last_datetime)+covering_spaces, end='\r', flush=True)

    # in the case that new data was gathered write it to disk
    if len(batches) > 1:
        df = pd.concat(batches, ignore_index=True)
        df.to_csv(f'data/{base}-{quote}.csv', index=False)
        return len(df.index) - old_lines
    return 0


def write_metadata(n_count):
    """write metadata dynamically so we can include a pair count"""

    metadata = {
        'id': 'jorijnsmit/binance-full-history',
        'title': 'Binance Full History',
        'subtitle': f'1 minute candlesticks for all {n_count} cryptocurrency pairs',
        'description': """### Introduction\n\nThis is nothing more than a collection of all 1 minute candlesticks on [Binance](https://binance.com). All {n_count} of them are included. Both gathering and uploading the data is fully automated---see [this GitHub repo](https://github.com/gosuto-ai/candlestick_scraper).\n\n### Content\n\nFor every trading pair all fields as returned by [Binance's official API endpoint for historical candlestick data](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data) are saved into their own csv file. First entry is the first timestamp available on the exchange, which is somewhere in 2017 for the longest running pairs.\n\nThe [Getting Started kernel](https://www.kaggle.com/jorijnsmit/getting-started-with-binance-s-candlesticks/) has a quick function that converts each column to its correct data type. It also shows a simple plot of one of the trading pairs with an added indicator and a volume plot.\n\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fb8664e6f26dc84e9a40d5a3d915c9640%2Fdownload.png?generation=1582053879538546&alt=media)\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fcd04ed586b08c1576a7b67d163ad9889%2Fdownload-1.png?generation=1582053899082078&alt=media)\n\n### Inspiration\n\nOne obvious use-case for this data could be general analysis such as adding technical indicators (moving averages, MACD, RSI, etc.) or creating correlation matrices. Other approaches could include backtesting trading algorithms or computing arbitrage potential with other exchanges.\n\n### License\n\nThis data is being collected automatically from cryptocurrency exchange Binance.""",
        'isPrivate': False,
        'licenses': [{'name': 'other'}],
        'keywords': [
            'finance',
            'computing',
            'investing',
            'currencies and foreign exchange'
        ],
        'collaborators': []
    }

    with open('data/dataset-metadata.json', 'w') as f:
        json.dump(metadata, f, indent=4)


def main():
    """main loop"""

    # get all pairs currently available
    all_symbols = pd.DataFrame(requests.get(f'{API_BASE}exchangeInfo').json()['symbols'])
    all_pairs = [tuple(x) for x in all_symbols[['baseAsset', 'quoteAsset']].to_records(index=False)]

    # randomising order helps during testing and doesn't make any difference in production
    random.shuffle(all_pairs)

    # do a full update on all currency pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair
        new_lines = all_candles_to_csv(base=base, quote=quote)
        if new_lines > 0:
            print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {base}-{quote}')
        else:
            print(f'{datetime.now()} {n}/{n_count} Already up to date with {base}-{quote}')

    # clean the data folder and upload a new version of the dataset to kaggle
    try:
        os.remove('data/.DS_Store')
    except FileNotFoundError:
        pass
    preprocessing.groom_data()
    write_metadata(n_count)
    yesterday = date.today() - timedelta(days=1)
    os.system(f'kaggle datasets version -p data/ -m "full update of {n_count} pairs up till {str(yesterday)}"')


if __name__ == '__main__':
    main()
