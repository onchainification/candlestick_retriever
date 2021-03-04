import os
from datetime import date
import pyarrow.parquet as pq
import pandas as pd
from main import SHAVE_OFF_TODAY

def set_dtypes(df):
    """
    set datetimeindex and convert all columns in pd.df to their proper dtype
    assumes csv is read raw without modifications; pd.read_csv(csv_filename)"""

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


def set_dtypes_compressed(df):
    """Create a `DatetimeIndex` and convert all critical columns in pd.df to a dtype with low
    memory profile. Assumes csv is read raw without modifications; `pd.read_csv(csv_filename)`."""

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


def quick_clean(df):
    """clean a raw dataframe"""

    # drop dupes
    dupes = df['open_time'].duplicated().sum()
    if dupes > 0:
        df = df[df['open_time'].duplicated() == False]

    # sort by timestamp, oldest first
    df.sort_values(by=['open_time'], ascending=False)

    # just a doublcheck
    assert_integrity(df)

    return df


def append_raw_to_parquet(df, full_path):
    """takes raw df and writes a parquet to disk"""
    df = polish_df(df)
    try:
        df = pd.concat([pq.read_pandas(full_path).to_pandas(), df])
    except OSError:
        pass
    df.to_parquet(full_path)

def write_raw_to_parquet(df, full_path):
    """takes raw df and writes a parquet to disk"""
    df = polish_df(df)
    df.to_parquet(full_path)

def polish_df(df):
    # some candlesticks do not span a full minute
    # these points are not reliable and thus filtered
    df = df[~(df['open_time'] - df['close_time'] != -59999)]

    # `close_time` column has become redundant now, as is the column `ignore`
    df = df.drop(['close_time', 'ignore'], axis=1)

    df = set_dtypes_compressed(df)

    # give all pairs the same nice cut-off
    if SHAVE_OFF_TODAY:
        df = df[df.index < str(date.today())]
    return df

def groom_data(dirname='data'):
    """go through data folder and perform a quick clean on all csv files"""

    for filename in os.listdir(dirname):
        if filename.endswith('.csv'):
            full_path = f'{dirname}/{filename}'
            quick_clean(pd.read_csv(full_path)).to_csv(full_path)


def compress_data(dirname='data'):
    """go through data folder and rewrite csv files to parquets"""

    os.makedirs('compressed', exist_ok=True)
    for filename in os.listdir(dirname):
        if filename.endswith('.csv'):
            full_path = f'{dirname}/{filename}'

            df = pd.read_csv(full_path)

            new_filename = filename.replace('.csv', '.parquet')
            new_full_path = f'compressed/{new_filename}'
            write_raw_to_parquet(df, new_full_path)
