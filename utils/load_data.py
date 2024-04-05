from typing import List

import pandas as pd

from simulator.simulator import AnonTrade, MdUpdate, OrderbookSnapshotUpdate


def load_trades(path, nrows=10000) -> List[AnonTrade]:
    trades = pd.read_csv(path + 'FIX.csv', nrows=nrows)
    trades = trades[['origin_time', 'received_time', 'side', 'quantity', 'price']].sort_values(
        ["origin_time", 'received_time'])
    receive_ts = trades.received_time.values
    exchange_ts = trades.origin_time.values
    trades = [AnonTrade(exchange_ts[i], receive_ts[i], trades.iloc[i].side.upper(), 
                        trades.iloc[i].quantity, trades.iloc[i].price) for i in range(len(trades))]
    return trades

def load_books(path, nrows=10000) -> List[OrderbookSnapshotUpdate]:
    lobs = pd.read_csv(path + 'FIX.csv', nrows=nrows)
    receive_ts = pd.to_datetime(lobs.received_time).values 
    exchange_ts = pd.to_datetime(lobs.received_time).values
    asks = [list(zip(lobs[f"ask_{i}_price"], lobs[f"ask_{i}_size"])) for i in range(20)]
    asks = [[asks[i][j] for i in range(len(asks))] for j in range(len(asks[0]))]
    bids = [list(zip(lobs[f"bid_{i}_price"], lobs[f"bid_{i}_size"])) for i in range(20)]
    bids = [[bids[i][j] for i in range(len(bids))] for j in range(len(bids[0]))]
    books = list(OrderbookSnapshotUpdate(*args) for args in zip(exchange_ts, receive_ts, asks, bids))
    return books

def merge_books_and_trades(books: List[OrderbookSnapshotUpdate], trades: List[AnonTrade]) -> List[MdUpdate]:
    '''
        This function merges lists of orderbook snapshots and trades 
    '''
    trades_dict = {(trade.exchange_ts, trade.receive_ts): trade for trade in trades}
    books_dict = {(book.exchange_ts, book.receive_ts): book for book in books}

    ts = sorted(trades_dict.keys() | books_dict.keys())

    md = [MdUpdate(*key, books_dict.get(key, None), trades_dict.get(key, None)) for key in ts]
    return md


def load_md_from_file(path: str, nrows=10000, btc=True) -> List[MdUpdate]:
    '''
        This function downloads orderbooks ans trades and merges them
    '''
    books = load_books(path, nrows, btc)
    trades = load_trades(path, nrows)
    return merge_books_and_trades(books, trades)
