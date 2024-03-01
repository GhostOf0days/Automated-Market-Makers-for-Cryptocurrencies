import pandas as pd
from data import *

def readCredentials(jsonPath:str) -> pd.DataFrame:
    """we use the tardis as data source, it is a paid resource. Please feel free to use it, but don't share it with others
    """
    apiKey = pd.read_json(jsonPath)['tardis']['apiKey']
    return apiKey


readCredentials(JSONPATH)