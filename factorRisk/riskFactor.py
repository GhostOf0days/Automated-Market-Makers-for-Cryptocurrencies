import numpy as np
import pandas as pd

STOREPath =  "..factorData/"


class riskFactor(object):

    def __init__(self, name, componentSymbols, freq='1m'):
        self._factorName = name
        self._symbols = componentSymbols
        self._freq = '1m'
        return

    @property
    def name(self):
        return self._factorName

    @property
    def symbols(self):
        return self._symbols

    @property
    def freq(self):
        return self._freq

    @property
    def factorRetTS(self):
        return self._factorTS

    def getFactorReturnTS(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate risk factor returns given frequency and symbols
        Parameters
        ----------
        data: a panel dataframe with all symbols and their corresponding returns

        Returns: a timeSeries table of risk factor returns and time
        -------

        """
        factorRet = data[data['symbol'].isin(self._symbols)].groupby("origin_time").agg({'return': np.mean})
        factorRet.rename(columns={'return': self._factorName}, inplace=True)
        self._factorTS = factorRet
        return

    def storeData(self):
        self._factorTS.to_csv(STOREPath + self._factorName + ".csv", index=False)
