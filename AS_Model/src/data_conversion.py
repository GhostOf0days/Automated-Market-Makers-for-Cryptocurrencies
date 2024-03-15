import numpy as np
import pandas as pd
from hftbacktest import HftBacktest, FeedLatency, Linear
from numba import njit


def data_create(file, best_bid_price:list, best_ask_price:list, best_bid_qty:list, best_ask_qty:list):
    #load the data
    data = np.load(file)
    print(list(data.keys()))
    book_data = data['data']
    data.close()

    #structure market depth snapshot 
    df = pd.DataFrame(book_data, columns=['event', 'exch_timestamp', 'local_timestamp', 'side', 'price', 'qty'])
    df['event'] = df['event'].astype(int)
    df['exch_timestamp'] = df['exch_timestamp'].astype(int)
    df['local_timestamp'] = df['local_timestamp'].astype(int)
    df['side'] = df['side'].astype(int)
    print(df)

    #backtesting to find best bid and best ask prices
    def add_bbo(hbt):
        # Iterating until hftbacktest reaches the end of data.
        while hbt.run:
            # Elapses 60-sec every iteration.
            # Time unit is the same as data's timestamp's unit.
            # timestamp of the sample data is in microseconds.
            if not hbt.elapse(60 * 1e6):
                # hftbacktest encounters the end of data while elapsing.
                return False

            # Prints the best bid and the best offer.
            bb = max(hbt.bid_depth.keys())
            ba = min(hbt.bid_depth.keys())

            best_bid_price.append(bb)
            best_ask_price.append(ba)
            best_bid_qty.append(hbt.bid_depth[bb])
            best_ask_qty.append(hbt.bid_depth[ba])
            ''''''
            '''
            print(
                #'current_timestamp:', hbt.current_timestamp,
                ', best_bid:', round(bb, 3),
                ', bid_qty:', hbt.bid_depth[bb],
                ', best_ask:', round(ba, 3),
                ', ask_qty:', hbt.bid_depth[ba], 
            )
            '''
        return True
    hbt = HftBacktest(
        book_data,
        tick_size=0.1,   # Tick size of a target trading asset
        lot_size=0.001,    # Lot size of a target trading asset, minimum trading unit.
        maker_fee=0.0002, # 0.02%, Maker fee, rebates if it is negative.
        taker_fee=0.0007, # 0.07%, Taker fee.
        order_latency=FeedLatency(), # Latency model: ConstantLatency, FeedLatency.
        asset_type=Linear, # Asset type: Linear, Inverse.
    )
    add_bbo(hbt)
