import asyncio
from tardis_client import TardisClient, Channel
from tardis_dev import datasets, get_exchange_details
import json
import csv
from dataUtils import *

tardisKey = readCredentials('./tardisCred.json')

DOWNLOADED = False
if __name__ == "__main__":
    if not DOWNLOADED:
        datasets.download(
            # one of https://api.tardis.dev/v1/exchanges with supportsDatasets:true - use 'id' value
            exchange="binance-futures",
            # accepted data types - 'datasets.symbols[].dataTypes' field in https://api.tardis.dev/v1/exchanges/deribit,
            # or get those values from 'deribit_details["datasets"]["symbols][]["dataTypes"] dict above
            data_types=["incremental_book_L2", "trades"],
            # change date ranges as needed to fetch full month or year for example
            from_date="2024-01-03",
            # to date is non inclusive
            to_date="2024-01-03",
            # accepted values: 'datasets.symbols[].id' field in https://api.tardis.dev/v1/exchanges/deribit
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT","LTCUSDT","XRPUSDT"],
            # (optional) your API key to get access to non sample data as well
            # (optional) path where data will be downloaded into, default dir is './datasets'
            download_dir="./datasets",
            api_key=tardisKey,
        )
    incL2Files = getFileNames("./datasets", 'incremental_book_L2')
    tradeFiles = getFileNames("./datasets", 'trades')
    '''
    coinNames = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT",
    "XRPUSDT", "SOLUSDT", "DOTUSDT", "LTCUSDT",
    "LINKUSDT", "DOGEUSDT", "MATICUSDT", "UNIUSDT",
    "TRXUSDT", "XLMUSDT", "BCHUSDT", "VETUSDT",
    "FILUSDT", "AAVEUSDT", "EOSUSDT", "XTZUSDT",
    ]
    '''
    coinNames = ["BTCUSDT", "ETHUSDT", "BNBUSDT","LTCUSDT","XRPUSDT"]
    dates = getDatesFromFiles(incL2Files)
    for coin in coinNames:
        for date in dates:
            incL2File = getFileNameByCoinAndDate(incL2Files,coin,date)
            tradeFile = getFileNameByCoinAndDate(tradeFiles,coin,date)
            wrtPath = getWrtPath(coin,date,'./datasets')
            prepareDataFromTardis(incL2File,tradeFile,wrtPath)
