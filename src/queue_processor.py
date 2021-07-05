import asyncio
import json
import logging

from src.util import decode_msg, queue_url_to_names, queue_urls_to_names
from src.wrapped_sqs import receive_messages, delete_messages, get_queue_list


class QueueProcessor:
    def __init__(self, config, aws_client, mongo_client):
        self._config = config
        self._aws_client = aws_client
        self._mongo_client = mongo_client

    async def store_to_mongo(self, collection, documents):
        db = self._mongo_client.test_database
        col = db[collection]
        logging.info(f'Stored to mongo {len(documents)} documents in {collection}')
        await col.insert_many(documents)

    async def process_queue(self, queue_url, batch_size=10):
        is_empty_queue = False
        while not is_empty_queue:
            received_messages = await receive_messages(aws_client=self._aws_client, queue_url=queue_url,
                                                       batch_size=batch_size)
            if received_messages.get('Messages'):
                logging.info(f'Got {len(received_messages["Messages"])} messages from {queue_url_to_names(queue_url)}')
                documents = [json.loads(decode_msg(msg['Body'])) for msg in received_messages['Messages']]
                await self.store_to_mongo(collection='test_collection', documents=documents)
                await delete_messages(aws_client=self._aws_client, queue_url=queue_url, messages=received_messages)
            else:
                logging.debug(f'Empty queue')
                is_empty_queue = True

    async def run_queues_processing(self):
        queue_urls = await get_queue_list(aws_client=self._aws_client, prefixes=[''])
        logging.info(f'run processing queues: {queue_urls_to_names(queue_urls)}')

        queue_processors = [
            asyncio.gather(*[self.process_queue(queue_url=queue_url)
                             for _ in range(self._config['WORKERS_PER_QUEUE'])])
            for queue_url in queue_urls
        ]
        await asyncio.gather(*queue_processors)
