# STILL HAVE TO FIX
import pickle
import pandas as pd

execution_latency = 10_000_000  # 10 ms
md_latency = 1_000_000  # 1 ms

with open('../data/features_dict.pickle', 'rb') as f:
    features_dict = pickle.load(f)

# Load factor exposure dataframes
# STILL HAVE TO ADD EXPOSURES FROM NOTEBOOK AS CSV FILES
BTCFactorExposure = pd.read_csv('../data/factorData/exposures/TCFactorExposure.csv')
BNBFactorExposure = pd.read_csv('../data/factorData/exposures/BNBFactorExposure.csv')
ETHFactorExposure = pd.read_csv('../data/factorData/exposures/ETHFactorExposure.csv')
XRPFactorExposure = pd.read_csv('../data/factorData/exposures/XRPFactorExposure.csv')

# Convert timestamp columns to datetime
BTCFactorExposure['origin_time'] = pd.to_datetime(BTCFactorExposure['origin_time'])
BNBFactorExposure['origin_time'] = pd.to_datetime(BNBFactorExposure['origin_time'])
ETHFactorExposure['origin_time'] = pd.to_datetime(ETHFactorExposure['origin_time'])
XRPFactorExposure['origin_time'] = pd.to_datetime(XRPFactorExposure['origin_time'])

# Set 'origin_time' as the index for each dataframe
BTCFactorExposure.set_index('origin_time', inplace=True)
BNBFactorExposure.set_index('origin_time', inplace=True)
ETHFactorExposure.set_index('origin_time', inplace=True)
XRPFactorExposure.set_index('origin_time', inplace=True)

md_updates = load_md_from_file('FIX')

sim = Sim(md_updates, features_dict, execution_latency, md_latency)

while True:
    receive_ts, updates, features = sim.tick()

    if updates is None:
        break
   
    # Get current factor exposures for each asset
    btc_exposure = BTCFactorExposure[BTCFactorExposure.index < receive_ts].iloc[-1]
    bnb_exposure = BNBFactorExposure[BNBFactorExposure.index < receive_ts].iloc[-1]
    eth_exposure = ETHFactorExposure[ETHFactorExposure.index < receive_ts].iloc[-1]
    xrp_exposure = XRPFactorExposure[XRPFactorExposure.index < receive_ts].iloc[-1]

    # Trading logic based on factor exposures and other features
    # STILL HAVE TO IMPLEMENT

    # Place orders using the simulator
    # sim.place_order(...)

# Analyze PnL 
info = get_pnl(updates)
print(info)