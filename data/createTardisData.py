import asyncio
from tardis_client import TardisClient, Channel
import json
import dataUtils
from . import JSONPATH

tardisKey = dataUtils.readCredentials(JSONPATH)
assert isinstance(tardisKey, str) == True

print(tardisKey)

filename = "put file name here"

print(tardisKey)

tardis_client = TardisClient(api_key = tardisKey)

async def replay():
    # replay method returns Async Generator
    messages = tardis_client.replay(
        exchange="exchange goes here", # examples: coinbase, binance
        from_date="from date goes here", # example: 2024-02-18
        to_date="to data goes here", # example: 2024-02-25
        filters=[Channel(name="csv file name", symbols=["symbol goes here"])], # example csv file: bitcoin_price_index.csv. example symbol: btscusdt.
        )
    
    with open("./csv_file_name.csv", mode="w") as csv_file: # example: ./bitcoin_price_index.cdv
        fieldnames = ["symbol", "price", "timestamp"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        async for local_timestamp, message in messages:
            data = message["params"]["data"]
            writer.writerow({"symbol": data["index_name"], "price": data["price"], "timestamp": data["timestamp"]})

    print("finished")

asyncio.run(replay())