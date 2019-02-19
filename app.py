from quart import Quart, g
import asyncio

from cosmos_rest_client import CosmosRestClient

endpoint = 'https://localhost:8081/'
key = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='

app = Quart(__name__)

async def get_client():
    if not hasattr(app, 'cosmos_client'):
        print("Creating client")
        app.cosmos_client = await CosmosRestClient.create(endpoint, key)
    return app.cosmos_client

@app.before_serving
async def warm_client():
    client = await get_client()
    _ = await client.get_item('test', 'test', 'test', 'test')

@app.after_serving
async def close_client():
    client = await get_client()
    await client.close()

@app.route('/')
async def hello():
    return 'hello'

@app.route('/db')
async def db():
    client = await get_client()
    r = await client.get_item('test', 'test', 'test', 'test')
    return str(r['resource'])

if __name__ == "__main__":
    app.run()