import asyncio

import math

def percentile(data, percentile):
    size = len(data)
    return sorted(data)[int(math.ceil((size * percentile) / 100)) - 1]

from cosmos_rest_client import CosmosRestClient

endpoint = 'https://localhost:8081/'
key = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='

MAX_REQUESTS = 500

durations = []

async def fetch_and_print(client, index, should_print=True):
    r = await client.get_item('test', 'test', 'test', 'test')
    r['index'] = index
    if(should_print):
        durations.append(r['duration'])
        # print(r)

async def main():
    client = await CosmosRestClient.create(endpoint, key)
    try: 
        tasks = []
        for i in range(MAX_REQUESTS):
            tasks.append(asyncio.ensure_future(fetch_and_print(client, i, False)))
        await asyncio.gather(*tasks)

        tasks = []
        for i in range(MAX_REQUESTS):
            await fetch_and_print(client, i)
            # tasks.append(asyncio.ensure_future(fetch_and_print(client, i)))
        await asyncio.gather(*tasks)
    finally:
        await client.close()

    print('median: ' + str(percentile(durations, 50)))
    print('p99: ' + str(percentile(durations, 99)))

loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.ensure_future(main()))