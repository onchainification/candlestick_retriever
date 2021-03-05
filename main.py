#!/usr/bin/env python
# coding: utf-8

"""Download historical candlestick data for all trading pairs on Binance.com.
All trading pair data is checked for integrity, sorted and saved as a Parquet
file and optionally as a CSV file.
The Parquet files are much more space efficient (~50GB vs ~10GB) and are
therefore the files used to upload to Kaggle after each run.

It is possible to circumvent the need for CSV buffers and update the parquet
files directly by setting CIRCUMVENT_CSV to True. This makes it easy to keep
the parquet files up to date after you have downloaded them from kaggle.
"""

__author__ = 'GOSUTO.AI'

import json
import os
import random
import subprocess
import time
from datetime import date, datetime, timedelta
from progressbar import ProgressBar
import pyarrow.parquet as pq
import requests
import pandas as pd
import preprocessing as pp

BATCH_SIZE = 1000  # Number of candles to ask for in each API request.
SHAVE_OFF_TODAY = False  # Whether to shave off candles after last midnight to equalize end-time of all datasets.
CIRCUMVENT_CSV = True  # Whether to use the parquet files directly when updating data.
UPLOAD_TO_KAGGLE = False  # Whether to upload the parquet files to kaggle after updating.

COMPRESSED_PATH = 'compressed'
CSV_PATH = 'data'
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

METADATA = {
    'id': 'jorijnsmit/binance-full-history',
    'title': 'Binance Full History',
    'isPrivate': False,
    'licenses': [{'name': 'other'}],
    'keywords': [
        'business',
        'finance',
        'investing',
        'currencies and foreign exchange'
    ],
    'collaborators': [],
    'data': []
}

def write_metadata(n_count):
    """Write the metadata file dynamically so we can include a pair count."""

    METADATA['subtitle'] = f'1 minute candlesticks for all {n_count} cryptocurrency pairs'
    METADATA['description'] = f"""### Introduction\n\nThis is a collection of all 1 minute candlesticks of all cryptocurrency pairs on [Binance.com](https://binance.com). All {n_count} of them are included. Both retrieval and uploading the data is fully automated—see [this GitHub repo](https://github.com/gosuto-ai/candlestick_retriever).\n\n### Content\n\nFor every trading pair, the following fields from [Binance's official API endpoint for historical candlestick data](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data) are saved into a Parquet file:\n\n```\n #   Column                        Dtype         \n---  ------                        -----         \n 0   open_time                     datetime64[ns]\n 1   open                          float32       \n 2   high                          float32       \n 3   low                           float32       \n 4   close                         float32       \n 5   volume                        float32       \n 6   quote_asset_volume            float32       \n 7   number_of_trades              uint16        \n 8   taker_buy_base_asset_volume   float32       \n 9   taker_buy_quote_asset_volume  float32       \ndtypes: datetime64[ns](1), float32(8), uint16(1)\n```\n\nThe dataframe is indexed by `open_time` and sorted from oldest to newest. The first row starts at the first timestamp available on the exchange, which is July 2017 for the longest running pairs.\n\nHere are two simple plots based on a single file; one of the opening price with an added indicator (MA50) and one of the volume and number of trades:\n\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fb8664e6f26dc84e9a40d5a3d915c9640%2Fdownload.png?generation=1582053879538546&alt=media)\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fcd04ed586b08c1576a7b67d163ad9889%2Fdownload-1.png?generation=1582053899082078&alt=media)\n\n### Inspiration\n\nOne obvious use-case for this data could be technical analysis by adding indicators such as moving averages, MACD, RSI, etc. Other approaches could include backtesting trading algorithms or computing arbitrage potential with other exchanges.\n\n### License\n\nThis data is being collected automatically from crypto exchange Binance."""

    with open(f'{COMPRESSED_PATH}/dataset-metadata.json', 'w') as file:
        json.dump(METADATA, file, indent=4)


def get_batch(symbol, interval='1m', start_time=0, limit=1000):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON into a pandas
    dataframe and return it. If not successful, return an empty dataframe.
    """

    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'limit': limit
    }
    try:
        # timeout should also be given as a parameter to the function
        response = requests.get(f'{API_BASE}klines', params, timeout=30)
    except requests.exceptions.ConnectionError:
        print('Connection error, Cooling down for 5 mins...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.Timeout:
        print('Timeout, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    
    except requests.exceptions.ConnectionResetError:
        print('Connection reset by peer, Cooling down for 5 min...')
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    if response.status_code == 200:
        return pd.DataFrame(response.json(), columns=LABELS)
    print(f'Got erroneous response back: {response}')
    return pd.DataFrame([])


def gather_new_candles(base, quote, last_timestamp, interval='1m'):
    """
    Gather all candlesticks available, starting from the last timestamp loaded from disk or from beginning of time.
    Stop if the timestamp that comes back from the api is the same as the last one.
    """
    previous_timestamp = None
    batches = [pd.DataFrame([], columns=LABELS)]

    first_read = True
    start_datetime = None
    bar = None
    print(f"Last timestamp of pair {base}-{quote} was {datetime.fromtimestamp(last_timestamp / 1000)}.")
    while previous_timestamp != last_timestamp:
        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base+quote,
            interval=interval,
            start_time=last_timestamp+1,
            limit=BATCH_SIZE
        )
        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch['open_time'].max()
        # sometimes no new trades took place yet on date.today();
        # in this case the batch is nothing new
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)

        #Get info for progressbar
        if first_read:
            start_datetime = datetime.fromtimestamp(new_batch['open_time'][0] / 1000)
            missing_data_timedelta = datetime.now() - start_datetime
            total_minutes_of_data = int(missing_data_timedelta.total_seconds()/60)
            print(f"Will fetch {missing_data_timedelta} ({total_minutes_of_data} minutes) worth of candles.")
            first_read = False
            if total_minutes_of_data >= BATCH_SIZE*2:
                bar = ProgressBar(max_value=total_minutes_of_data)

        if bar is not None:
            time_covered = datetime.fromtimestamp(last_timestamp / 1000) - start_datetime
            bar.update(int(time_covered.total_seconds()/60))

    return batches


def all_candles_to_csv(base, quote, interval='1m'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to CSV.
    """
    filepath = f'{CSV_PATH}/{base}-{quote}.csv'

    last_timestamp, old_lines = get_csv_info(filepath)
    new_candle_batches = gather_new_candles(base, quote, last_timestamp, interval)
    return write_to_csv(filepath, new_candle_batches, old_lines)


def all_candles_to_parquet(base, quote, interval='1m'):
    """Collect a list of candlestick batches with all candlesticks of a trading pair,
    concat into a dataframe and write it to parquet.
    """
    filepath = f'{COMPRESSED_PATH}/{base}-{quote}.parquet'

    last_timestamp, old_lines = get_parquet_info(filepath)
    new_candle_batches = gather_new_candles(base, quote, last_timestamp, interval)
    return write_to_parquet(filepath, new_candle_batches, base, quote, append=True)


def get_csv_info(filepath):
    """
    Reads and returns the last timestamp and number of candles in a csv file.
    """
    last_timestamp = 0
    old_lines = 0
    try:
        batches = [pd.read_csv(filepath)]
        last_timestamp = batches[-1]['open_time'].max()
        old_lines = len(batches[-1].index)
    except FileNotFoundError:
        pass
    return last_timestamp, old_lines


def get_parquet_info(filepath):
    """
    Reads and returns the last timestamp and number of candles in a parquet file.
    """
    last_timestamp = 0
    old_lines = 0
    try:
        existing_data = pq.read_pandas(filepath).to_pandas()
        if not existing_data.empty:
            last_timestamp = int(existing_data.index.max().timestamp()*1000)
            old_lines = len(existing_data.index)
    except OSError:
        pass
    return last_timestamp, old_lines


def write_to_parquet(file, batches, base, quote, append=False):
    """
    Writes a batch of candles data to a parquet file.
    """
    df = pd.concat(batches, ignore_index=True)
    df = pp.quick_clean(df)
    if append:
        pp.append_raw_to_parquet(df, file)
    else:
        pp.write_raw_to_parquet(df, file)
    METADATA['data'].append({
        'description': f'All trade history for the pair {base} and {quote} at 1 minute intervals. '
                       f'Counts {df.index.size} records.',
        'name': f"{base}-{quote}",
        'totalBytes': os.stat(file).st_size,
        'columns': []
    })
    return len(df.index)


def write_to_csv(file, batches, old_lines):
    """
    Writes a batch of candles data to a csv file.
    """
    if len(batches) > 0:
        df = batches_to_pd(batches)
        header = not os.path.isfile(file)
        df.to_csv(file, index=False, mode='a', header=header)
        return len(df.index) - old_lines
    return 0


def batches_to_pd(batches):
    """
    Converts batches of candle data to a pandas dataframe.
    """
    df = pd.concat(batches, ignore_index=True)
    return pp.quick_clean(df)


def csv_to_parquet(base, quote):
    """
    Saves a csv file given by a base and a quote to a parquet file.
    """
    csv_filepath = f'{CSV_PATH}/{base}-{quote}.csv'
    parquet_filepath = f'{COMPRESSED_PATH}/{base}-{quote}.parquet'
    data = [pd.read_csv(csv_filepath)]
    write_to_parquet(parquet_filepath, data, base, quote)


def main():
    """Main loop; loop over all currency pairs that exist on the exchange. Once done upload the
    compressed (Parquet) dataset to Kaggle.
    """

    # get all pairs currently available
    all_symbols = pd.DataFrame(requests.get(f'{API_BASE}exchangeInfo').json()['symbols'])
    all_pairs = [tuple(x) for x in all_symbols[['baseAsset', 'quoteAsset']].to_records(index=False)]
    print(f'{datetime.now()} Got {len(all_pairs)} pairs from binance.')

    # randomising order helps during testing and doesn't make any difference in production
    random.shuffle(all_pairs)

    # make sure data folders exist
    os.makedirs(f'{CSV_PATH}', exist_ok=True)
    os.makedirs(f'{COMPRESSED_PATH}', exist_ok=True)

    # do a full update on all pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair
        print(f'{datetime.now()} {n}/{n_count} Updating {base}-{quote}')
        if CIRCUMVENT_CSV:
            new_lines = all_candles_to_parquet(base=base, quote=quote)
        else:
            new_lines = all_candles_to_csv(base=base, quote=quote)
        if new_lines > 0:
            print(f'{datetime.now()} {n}/{n_count} Wrote {new_lines} new lines to file for {base}-{quote}')
        else:
            print(f'{datetime.now()} {n}/{n_count} Already up to date with {base}-{quote}')

    if UPLOAD_TO_KAGGLE:
        # clean the data folder and upload a new version of the dataset to kaggle
        try:
            os.remove(f'{COMPRESSED_PATH}/.DS_Store')
        except FileNotFoundError:
            pass
        write_metadata(n_count)
        yesterday = date.today() - timedelta(days=1)
        subprocess.run(['kaggle', 'datasets', 'version', '-p', f'{COMPRESSED_PATH}/', '-m', f'full update of all {n_count} pairs up to {str(yesterday)}'])
        os.remove(f'{COMPRESSED_PATH}/dataset-metadata.json')


if __name__ == '__main__':
    main()
