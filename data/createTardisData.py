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
        filters=[Channel(name="depth", symbols=["symbol goes here"])] # example: btscusdt
    )

    async with open(filename, "w") as message_file:
        # Messages as provided by the exchange's real-time stream
        async for local_timestamp, message in messages:
            message_file.write(json.dumps(message) + "\n")


asyncio.run(replay())