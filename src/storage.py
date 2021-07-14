import datetime
import json
import logging
from pathlib import Path

import aiofiles
import motor.motor_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


class Storage:
    def __init__(self, config):
        self._mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config['MONGODB']['URL'])
        self._es_client = AsyncElasticsearch([config['ELASTICSEARCH']['URL']])
        self._files_path = config['FILES']['PATH']

    async def store_to_mongo(self, database, collection, documents):
        try:
            db = self._mongo_client[database]
            col = db[collection]
            await col.insert_many(documents)
            logging.info(f'Stored to mongo {len(documents)} documents in {collection}')
        except Exception as error:
            # logging.exception(f'Can\'t store to mongo', exc_info=False)
            raise error

    async def store_to_es(self, index, documents):
        try:
            res = await async_bulk(self._es_client, ({'_index': index, 'doc': d} for d in documents))
            logging.info(f'Stored to es {res[0]} documents in {index}')
        except Exception as error:
            logging.exception('Can\'t store to elasticsearch', exc_info=False)
            raise error

    async def store_to_files(self, directory, filename_prefix, documents):
        try:
            date = datetime.datetime.utcnow().replace(second=0, microsecond=0).strftime("%Y-%m-%d-%H-%M")
            filename = f'{self._files_path}/{directory}/{filename_prefix}__{date}.json'
            Path(f'{self._files_path}/{directory}').mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(filename, 'a') as file:
                for d in documents:
                    await file.write(f'{json.dumps(d)}\n')
            logging.info(f'Stored to file {filename} {len(documents)} documents')
        except Exception as error:
            logging.exception('Can\'t store to file', exc_info=False)
            raise error

    async def store(self, routes, documents):
        if 'mongo' in routes:
            await self.store_to_mongo(database=routes['mongo']['database'],
                                      collection=routes['mongo']['space'],
                                      documents=documents)
        elif 'elasticsearch' in routes:
            await self.store_to_es(index=routes['elasticsearch']['space'],
                                   documents=documents)
        elif 'file' in routes:
            await self.store_to_files(directory=routes['elasticsearch']['database'],
                                      filename_prefix=routes['elasticsearch']['space'],
                                      documents=documents)
