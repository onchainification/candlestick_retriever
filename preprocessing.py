import os
from datetime import datetime

import pandas as pd

def set_dtypes(df):
    """
    set datetimeindex and convert all columns in pd.df to their proper dtype
    assumes csv is read raw without modifications; pd.read_csv(csv_filename)
    """

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


def groom_data(dirname='data'):
    """go through data folder and perform a quick clean on all csv files"""

    for filename in os.listdir(dirname):
        if filename.endswith('.csv'):
            full_path = f'{dirname}/{filename}'
            quick_clean(pd.read_csv(full_path)).to_csv(full_path)
