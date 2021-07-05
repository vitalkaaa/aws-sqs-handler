import logging

import motor.motor_asyncio
from elasticsearch import AsyncElasticsearch


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

    async def store_to_es(self):
        pass

    async def store_to_files(self):
        pass
