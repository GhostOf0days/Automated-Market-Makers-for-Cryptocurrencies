import pandas as pd
import numpy as np

from fpm_risk_model.statistics import PCA
from fpm_risk_model.covariance import ShrunkCovariance

def calculate_systemic_risk(book_data, trade_data):
    # Preprocess order book data
    book_data = book_data.sort_values(['timestamp'])
    book_data['mid_price'] = (book_data['best_ask'] + book_data['best_bid']) / 2

    # Preprocess trade data
    trade_data = trade_data.sort_values(['timestamp'])
    trade_data['price'] = trade_data['price'].astype(float)
    trade_data['size'] = trade_data['size'].astype(float)
    trade_data['dollar_value'] = trade_data['price'] * trade_data['size']

    # Calculate size factors
    book_data['MCAP'] = book_data['mid_price'] * book_data['total_volume']
    book_data['MCAP'] = np.log(book_data['MCAP'])
    book_data['PRC'] = np.log(book_data['mid_price'])
    book_data['MAXDPRC'] = book_data['mid_price'].rolling(7).max()
    book_data['AGE'] = (pd.Timestamp.now() - book_data['timestamp']) / np.timedelta64(1, 'W')

    # Calculate momentum factors
    book_data['r_1_0'] = book_data['mid_price'].pct_change(1)
    book_data['r_2_0'] = book_data['mid_price'].pct_change(2)

    risk_factors = ['MCAP', 'PRC', 'MAXDPRC', 'AGE', 'r_1_0', 'r_2_0']

    instrument_returns = book_data['mid_price'].pct_change()

    # Create a PCA risk model
    risk_model = PCA(n_components=5)
    risk_model.fit(X=instrument_returns)

    factor_exposures = risk_model.factor_exposures_

    # Transform the risk model with instrument returns
    risk_model.transform(y=instrument_returns)

    # Calculate covariance matrix using shrinkage estimator
    covariance_estimator = ShrunkCovariance(shrinkage_target='diagonal')
    covariance_matrix = covariance_estimator.fit(risk_model.factor_returns_).covariance_

    # Implement systemic risk calculation logic
    systemic_risk_scores = calculate_systemic_risk_scores(factor_exposures, covariance_matrix)

    return systemic_risk_scores

def calculate_systemic_risk_scores(factor_exposures, covariance_matrix):
    systemic_risk_scores = factor_exposures.dot(np.diag(covariance_matrix))

    return systemic_risk_scores