METADATA = {
    "id": "jorijnsmit/binance-full-history",
    "title": "Binance Full History",
    "isPrivate": False,
    "licenses": [{"name": "other"}],
    "keywords": ["business", "finance", "investing", "currencies and foreign exchange"],
    "collaborators": [],
    "data": [],
}


def write_metadata(n_count):
    """Write the metadata file dynamically so we can include a pair count."""

    METADATA[
        "subtitle"
    ] = f"1 minute candlesticks for all {n_count} cryptocurrency pairs"
    METADATA[
        "description"
    ] = f"""### Introduction\n\nThis is a collection of all 1 minute candlesticks of all cryptocurrency pairs on [Binance.com](https://binance.com). All {n_count} of them are included. Both retrieval and uploading the data is fully automated—see [this GitHub repo](https://github.com/gosuto-ai/candlestick_retriever).\n\n### Content\n\nFor every trading pair, the following fields from [Binance's official API endpoint for historical candlestick data](https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data) are saved into a Parquet file:\n\n```\n #   Column                        Dtype         \n---  ------                        -----         \n 0   open_time                     datetime64[ns]\n 1   open                          float32       \n 2   high                          float32       \n 3   low                           float32       \n 4   close                         float32       \n 5   volume                        float32       \n 6   quote_asset_volume            float32       \n 7   number_of_trades              uint16        \n 8   taker_buy_base_asset_volume   float32       \n 9   taker_buy_quote_asset_volume  float32       \ndtypes: datetime64[ns](1), float32(8), uint16(1)\n```\n\nThe dataframe is indexed by `open_time` and sorted from oldest to newest. The first row starts at the first timestamp available on the exchange, which is July 2017 for the longest running pairs.\n\nHere are two simple plots based on a single file; one of the opening price with an added indicator (MA50) and one of the volume and number of trades:\n\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fb8664e6f26dc84e9a40d5a3d915c9640%2Fdownload.png?generation=1582053879538546&alt=media)\n![](https://www.googleapis.com/download/storage/v1/b/kaggle-user-content/o/inbox%2F2234678%2Fcd04ed586b08c1576a7b67d163ad9889%2Fdownload-1.png?generation=1582053899082078&alt=media)\n\n### Inspiration\n\nOne obvious use-case for this data could be technical analysis by adding indicators such as moving averages, MACD, RSI, etc. Other approaches could include backtesting trading algorithms or computing arbitrage potential with other exchanges.\n\n### License\n\nThis data is being collected automatically from crypto exchange Binance."""

    with open(f"{COMPRESSED_PATH}/dataset-metadata.json", "w") as file:
        json.dump(METADATA, file, indent=4)


# clean the data folder and upload a new version of the dataset to kaggle
try:
    os.remove(f"{COMPRESSED_PATH}/.DS_Store")
except FileNotFoundError:
    pass
write_metadata(n_count)
yesterday = date.today() - timedelta(days=1)
subprocess.run(
    [
        "kaggle",
        "datasets",
        "version",
        "-p",
        f"{COMPRESSED_PATH}/",
        "-m",
        f"full update of all {n_count} pairs up to {str(yesterday)}",
    ]
)
os.remove(f"{COMPRESSED_PATH}/dataset-metadata.json")
