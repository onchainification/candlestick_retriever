import os
from datetime import date
import pyarrow.parquet as pq
import pandas as pd


def set_dtypes_compressed(df):
    """Create a `DatetimeIndex` on a raw pd.df and convert all critical columns
    to a dtype with low memory profile."""

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df = df.set_index('open_time', drop=True)

    df = df.astype(dtype={
        'open': 'float32',
        'high': 'float32',
        'low': 'float32',
        'close': 'float32',
        'volume': 'float32',
        'number_of_trades': 'uint16',
        'quote_asset_volume': 'float32',
        'taker_buy_base_asset_volume': 'float32',
        'taker_buy_quote_asset_volume': 'float32'
    })

    return df


def assert_integrity(df):
    """make sure no rows have empty cells or duplicate timestamps exist"""

    assert df.isna().all(axis=1).any() == False
    assert df['open_time'].duplicated().any() == False


def clean_raw(df, limit_to_today=True):
    """Clean a raw dataframe from duplicates, sort by timestamp, filter
    incomplete candles, drop redundant columns, set the correct dtype and
    (optionally) cut off the data for the current day."""

    # drop dupes
    dupes = df['open_time'].duplicated().sum()
    if dupes > 0:
        df = df[df['open_time'].duplicated() == False]

    # sort by timestamp, oldest first
    df.sort_values(by=['open_time'], ascending=False)

    # some candlesticks do not span a full minute
    # these points are not reliable and thus filtered
    df = df[~(df['open_time'] - df['close_time'] != -59999)]

    # `close_time` column has become redundant now, as is the column `ignore`
    df = df.drop(['close_time', 'ignore'], axis=1)

    # just a doublcheck on nans and duplicate timestamps
    assert_integrity(df)

    df = set_dtypes_compressed(df)

    # give all pairs the same nice cut-off
    if limit_to_today:
        df = df[df.index < str(date.today())]

    return df


def write_raw_to_parquet(df, full_path, limit_to_today=True, append=False):
    """Takes raw df and writes it to a parquet file, either overwriting existing
    data or appending to it. If the file does not exist, it is created."""
    df = clean_raw(df, limit_to_today)
    if append:
        try:
            df = pd.concat([pd.read_parquet(full_path), df])
        except OSError:
            pass
    df.to_parquet(full_path)
