import base64
import hmac
from hashlib import sha256
import ssl
import datetime
import aiohttp
from urllib.parse import quote

def get_auth_header_using_master_key(
    verb,
    resource_id_or_fullname,
    resource_type,
    date,
    master_key):
    """Gets the authorization token using `master_key.
    :param str verb:
    :param str resource_id_or_fullname:
    :param str resource_type:
    :param dict headers:
    :param str master_key:
    :return:
        The authorization token.
    :rtype: dict
    """

    # decodes the master key which is encoded in base64    
    key = base64.b64decode(master_key)
    
    # Skipping lower casing of resource_id_or_fullname since it may now contain "ID" of the resource as part of the fullname
    text = '{verb}\n{resource_type}\n{resource_id_or_fullname}\n{date}\n\n'.format(
        verb=(verb.lower() or ''),
        resource_type=(resource_type.lower() or ''),
        resource_id_or_fullname=(resource_id_or_fullname or ''),
        date=date.lower())

    body = text.encode('utf-8')
    digest = hmac.digest(key, body, sha256)
    signature = base64.encodebytes(digest).decode('utf-8')

    master_token = 'master'
    token_version = '1.0'
    return  quote('type={type}&ver={ver}&sig={sig}'.format(type=master_token,
                                                    ver=token_version,
                                                    sig=signature[:-1]))

class CosmosRestClient:
    def __init__(self, endpoint, key):
        self.endpoint = endpoint
        self.key = key

    @staticmethod
    async def create(endpoint, key):
        client = CosmosRestClient(endpoint, key)
        conn = aiohttp.TCPConnector(verify_ssl=False, limit=1000)
        client.session = aiohttp.ClientSession(connector=conn)
        return client

    async def request(self, path, link, method, type, pk):
        url = f'{self.endpoint}{path}'
        start = datetime.datetime.now()
        date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        auth = get_auth_header_using_master_key(method, link, type, date, self.key)

        headers = {
            'x-ms-date': date,
            'x-ms-version': '2018-06-18',
            'authorization': auth,
        }

        if(pk is not None):
            headers['x-ms-documentdb-partitionkey'] = pk

        async with self.session.get(url, headers=headers) as r:
            return {'resource': await r.json(), 'status': r.status, 'duration': (datetime.datetime.now() - start).total_seconds() * 1000}
    
    async def get_item(self, database_id, container_id, item_id, pk):
        path = f'dbs/{database_id}/colls/{container_id}/docs/{item_id}'
        return await self.request(path, path, 'GET', 'docs', f'["{pk}"]')

    async def close(self):
        await self.session.close()
