import pandas as pd
import re
from hftbacktest.data.utils import tardis
import glob
import numpy as np
import os

def readCredentials(jsonPath:str) -> pd.DataFrame:
    """we use the tardis as data source, it is a paid resource. Please feel free to use it, but don't share it with others
    """
    apiKey = pd.read_json(jsonPath)['tardis']['apiKey']
    return apiKey


def prepareDataFromTardis(incrementalPath:str, tradePath:str, wrtPath:str):
    """

    Parameters
    ----------
    incrementalPath: ./datasets/binance-futures_incremental_book_L2_2024-01-02_BTCUSDT.csv.gz
    tradePath:./datasets/binance-futures_trades_2024-01-02_BTCUSDT.csv.gz
    Returns
    -------

    """
    tardis.convert(
        [incrementalPath, tradePath],
        output_filename = wrtPath,
        buffer_size=200_000_000
    )

def getFileNames(dataFolder:str,dtype:str):
    assert dtype in ['incremental_book_L2','trades']
    if dtype == 'incremental_book_L2':
        fileNames = glob.glob(dataFolder+"/*_incremental_book_L2_*")
    else:
        fileNames = glob.glob(dataFolder+ "/*_trades_*")

    return fileNames

def getDatesFromFiles(allFiles:list)->list:
    dates = set()
    for file in allFiles:
        date = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", file).group(1)
        dates.add(date)
    return list(dates)

def getWrtPath(coin:str,date:str,base_dir:str)->str:
    #path check
    #print("path: ", header + "/"+coin+"_"+date+".npz")
    coin_dir = os.path.join(base_dir, coin)
    os.makedirs(coin_dir, exist_ok=True)
    return os.path.join(coin_dir, f"{coin}_{date}.npz")

def getFileNameByCoinAndDate(fileNames:list,coin:str,date:str)->str:
    for file in fileNames:
        if coin in file and date in file:
            return file
    return 'None'
