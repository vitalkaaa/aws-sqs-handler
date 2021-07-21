import datetime
import json
import logging
from pathlib import Path

import aiofiles
import motor.motor_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from config import Config


class Storage:
    def __init__(self, config: Config):
        self._mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config['MONGODB']['URL'])
        self._es_client = AsyncElasticsearch([config['ELASTICSEARCH']['URL']])
        self._files_path = config['FILES']['PATH']

    async def store_to_mongo(self, database: str, collection: str, documents: list) -> None:
        try:
            db = self._mongo_client[database]
            col = db[collection]
            await col.insert_many(documents)
            logging.info(f'Stored to mongo {len(documents)} documents in {collection}')
        except Exception as error:
            raise error

    async def store_to_es(self, index: str, documents: list) -> None:
        try:
            res = await async_bulk(self._es_client, ({'_index': index, 'doc': d} for d in documents))
            logging.info(f'Stored to es {res[0]} documents in {index}')
        except Exception as error:
            raise error

    async def store_to_files(self, directory: str, filename_prefix: str, documents: list) -> None:
        try:
            date = datetime.datetime.utcnow().replace(second=0, microsecond=0).strftime("%Y-%m-%d-%H-%M")
            filename = f'{self._files_path}/{directory}/{filename_prefix}__{date}.json'
            Path(f'{self._files_path}/{directory}').mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(filename, 'a') as file:
                for d in documents:
                    await file.write(f'{json.dumps(d)}\n')
            logging.info(f'Stored to file {filename} {len(documents)} documents')
        except Exception as error:
            raise error

    async def store(self, routes: dict, documents: list) -> None:
        if not routes:
            raise Exception('Empty storage routes')

        if 'mongo' in routes:
            await self.store_to_mongo(database=routes['mongo']['database'],
                                      collection=routes['mongo']['space'],
                                      documents=documents)
        if 'elasticsearch' in routes:
            await self.store_to_es(index=routes['elasticsearch']['space'],
                                   documents=documents)
        if 'file' in routes:
            await self.store_to_files(directory=routes['file']['database'],
                                      filename_prefix=routes['file']['space'],
                                      documents=documents)
