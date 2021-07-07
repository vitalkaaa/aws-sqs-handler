import datetime
import json
import logging

import aiofiles
import motor.motor_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


class Storage:
    def __init__(self, config):
        self._mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config['MONGODB']['URL'])
        self._es_client = AsyncElasticsearch([config['ELASTICSEARCH']['URL']])
        self._files_path = config['FILES']['PATH']

    async def store_to_mongo(self, collection, documents):
        db = self._mongo_client.test_database
        col = db[collection]
        logging.info(f'Stored to mongo {len(documents)} documents in {collection}')
        await col.insert_many(documents)

    async def store_to_es(self, index, documents):
        res = await async_bulk(self._es_client, ({'_index': index, 'doc': d} for d in documents))
        logging.info(f'Stored to es {res[0]} documents in {index}')

    async def store_to_files(self, filename_prefix, documents):
        timestamp = datetime.datetime.utcnow().replace(second=0, microsecond=0).strftime("%Y%m%d-%H%M%S")
        filename = f'{self._files_path}/{filename_prefix}-{timestamp}.json'
        async with aiofiles.open(filename, 'a') as file:
            for d in documents:
                await file.write(f'{json.dumps(d)}\n')
        logging.info(f'Stored to file {filename} {len(documents)} documents')
