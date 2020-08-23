# candlestick_retriever
Retrieve all historical candlestick data from crypto exchange Binance and upload it to Kaggle.

## Dependencies

- `pandas`
- `requests`
- `pyarrow`
- `kaggle`

## Running

Simply run `./main.py` to either download or update every single pair available:

```
[...]
2020-08-22 17:44:24.178846 959/970 Wrote 83000 new lines to file for DOGE-BTC 
2020-08-22 17:45:13.963455 960/970 Wrote 83000 new lines to file for NULS-ETH 
2020-08-22 17:45:14.573595 961/970 Already up to date with BTCB-BTC
2020-08-22 17:46:06.781870 962/970 Wrote 83000 new lines to file for ATOM-BTC 
2020-08-22 17:46:08.669972 963/970 Already up to date with LSK-BNB
[...]
```

Once that is completed you should end up with a directory with a Parquet file for each pair, currently 970 files totaling ~12GB.
