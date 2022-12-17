#!/usr/bin/env python
# coding: utf-8

"""
Download historical candlestick data for all trading pairs on Binance.com.
All trading pair data is checked for integrity, sorted and saved as a Parquet
file.
"""

__author__ = "GOSUTO.AI"
__version__ = "2.0.0"

import os
import random
import time
from datetime import datetime

import pandas as pd
import preprocessing as pp
import requests
from progressbar import ProgressBar


# check whether script being run in PyCharm environment
IN_PYCHARM = "PYCHARM_HOSTED" in os.environ

BATCH_SIZE = 1000  # number of candles to fetch per API request
SHAVE_OFF_TODAY = True  # shave off candles after last midnight to equalize end-time for all trading pairs
SKIP_DELISTED = False
DATA_PATH = "data"

API_BASE = "https://api.binance.com/api/v3/"

LABELS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


def get_batch(symbol, interval="1m", start_time=0, limit=1000):
    """Use a GET request to retrieve a batch of candlesticks. Process the JSON
    into a pandas dataframe and return it. If not successful, return an empty
    dataframe.
    """

    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "limit": limit,
    }

    try:
        response = requests.get(f"{API_BASE}klines", params, timeout=30)
    except requests.exceptions.ConnectionError:
        print("Connection error, Cooling down for 5 mins...")
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    except requests.exceptions.Timeout:
        print("Timeout, Cooling down for 5 min...")
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)
    except requests.exceptions.ConnectionResetError:
        print("Connection reset by peer, Cooling down for 5 min...")
        time.sleep(5 * 60)
        return get_batch(symbol, interval, start_time, limit)

    if response.status_code == 200:
        return pd.DataFrame(response.json(), columns=LABELS)
    print(f"Got erroneous response back: {response}")
    return pd.DataFrame([])


def gather_new_candles(base, quote, last_timestamp, interval="1m", n=0, n_count=0):
    """Gather all candlesticks available, starting from the last timestamp
    loaded from disk or from beginning of time. Stop if the timestamp that comes
    back from the api is the same as the last one.
    """
    previous_timestamp = None
    batches = [pd.DataFrame([], columns=LABELS)]

    first_read = True
    start_datetime = None
    bar = None
    if last_timestamp == 0:
        print(
            f"{datetime.now()} {n:04d}/{n_count} Starting from the beginning (no last known timestamp)"
        )
    else:
        print(
            f"{datetime.now()} {n:04d}/{n_count} Starting from last known timestamp ({datetime.fromtimestamp(last_timestamp / 1000)})"
        )
    while previous_timestamp != last_timestamp:
        previous_timestamp = last_timestamp

        new_batch = get_batch(
            symbol=base + quote,
            interval=interval,
            start_time=last_timestamp + 1,
            limit=BATCH_SIZE,
        )
        # requesting candles from the future returns empty
        # also stop in case response code was not 200
        if new_batch.empty:
            break

        last_timestamp = new_batch["open_time"].max()
        # sometimes no new trades took place yet on date.today();
        # in this case the batch contains no new data
        if previous_timestamp == last_timestamp:
            break

        batches.append(new_batch)

        # get info for progressbar
        if first_read:
            start_datetime = datetime.fromtimestamp(new_batch["open_time"][0] / 1000)
            missing_data_timedelta = datetime.now() - start_datetime
            total_minutes_of_data = int(missing_data_timedelta.total_seconds() / 60)
            if total_minutes_of_data > 1440:
                missing_data_timedelta = str(missing_data_timedelta).split(",")[0]
            else:
                missing_data_timedelta = "24 hours"
            print(
                f"{datetime.now()} {n:04d}/{n_count} Fetching all available data from last {missing_data_timedelta} (max {total_minutes_of_data} candles)"
            )
            if IN_PYCHARM:
                time.sleep(0.2)
            first_read = False
            if total_minutes_of_data >= BATCH_SIZE * 2:
                bar = ProgressBar(max_value=total_minutes_of_data).start()

        if bar is not None:
            time_covered = (
                datetime.fromtimestamp(last_timestamp / 1000) - start_datetime
            )
            minutes_covered = int(time_covered.total_seconds() / 60)
            bar.max_value = max(bar.max_value, minutes_covered)
            bar.update(minutes_covered)
    if bar is not None:
        bar.finish(dirty=True)
    if IN_PYCHARM:
        time.sleep(0.2)
    return batches


def all_candles_to_parquet(base, quote, interval="1m", n=0, n_count=0):
    """Collect a list of candlestick batches with all candlesticks of a trading
    pair, concat into a dataframe and write it to parquet.
    """
    filepath = f"{DATA_PATH}/{base}-{quote}.parquet"

    last_timestamp, _ = get_parquet_info(filepath)
    new_candle_batches = gather_new_candles(
        base, quote, last_timestamp, interval, n, n_count
    )
    return write_to_parquet(filepath, new_candle_batches, base, quote, append=True)


def get_parquet_info(filepath):
    """Reads and returns the last timestamp and number of candles in a parquet
    file.
    """
    last_timestamp = 0
    old_lines = 0
    try:
        existing_data = pd.read_parquet(filepath)
        if not existing_data.empty:
            last_timestamp = int(existing_data.index.max().timestamp() * 1000)
            old_lines = len(existing_data.index)
    except OSError:
        pass
    return last_timestamp, old_lines


def write_to_parquet(file, batches, append=False):
    """Write batches of candle data to a parquet file."""
    df = pd.concat(batches, ignore_index=True)
    pp.write_raw_to_parquet(df, file, SHAVE_OFF_TODAY, append=append)
    return len(df.index)


def main():
    """Main loop; loop over all currency pairs that exist on the exchange."""
    # get all pairs currently available
    all_symbols = pd.DataFrame(
        requests.get(f"{API_BASE}exchangeInfo").json()["symbols"]
    )
    active_symbols = all_symbols.loc[all_symbols["status"] == "TRADING"]
    if SKIP_DELISTED:
        all_pairs = [
            tuple(x)
            for x in active_symbols[["baseAsset", "quoteAsset"]].to_records(index=False)
        ]
        n_inactive = len(all_symbols) - len(active_symbols)
        print(
            f"{datetime.now()} Got {len(all_pairs)} active pairs from Binance. Dropped {n_inactive} inactive pairs."
        )
    else:
        all_pairs = [
            tuple(x)
            for x in all_symbols[["baseAsset", "quoteAsset"]].to_records(index=False)
        ]
        print(
            f"{datetime.now()} Got {len(all_pairs)} pairs from Binance, of which {len(active_symbols)} are active."
        )

    # randomising order helps during testing and doesn't make any difference in production
    random.shuffle(all_pairs)

    # make sure data folders exist
    os.makedirs(f"{DATA_PATH}", exist_ok=True)

    # do a full update on all pairs
    n_count = len(all_pairs)
    for n, pair in enumerate(all_pairs, 1):
        base, quote = pair
        print(f"{datetime.now()} {n:04d}/{n_count} Updating {base}-{quote}")
        new_lines = all_candles_to_parquet(base=base, quote=quote, n=n, n_count=n_count)
        if new_lines > 0:
            print(
                f"{datetime.now()} {n:04d}/{n_count} Wrote {new_lines} new candles to file for {base}-{quote}"
            )
        else:
            print(
                f"{datetime.now()} {n:04d}/{n_count} Already up to date with {base}-{quote}"
            )


if __name__ == "__main__":
    main()
