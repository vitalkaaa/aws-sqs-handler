import asyncio
import json
import logging
from itertools import chain

from src.config import Config
from src.storage import Storage
from src.util import decode_msg, queue_url_to_name, queue_urls_to_names, encode_msg, encode_tag, decode_tag
from src.wrapped_sqs import WrappedSQS


class QueuesProcessor:
    def __init__(self, sqs, storage, config):
        self._config: Config = config
        self._storage: Storage = storage
        self._sqs: WrappedSQS = sqs

    async def process_queue(self, queue_url, tags, batch_size=10):
        routes = decode_tag(tags['routes'])
        print(routes)
        is_empty_queue = False
        while not is_empty_queue:
            received_messages = await self._sqs.receive_messages(queue_url=queue_url, batch_size=batch_size)
            if received_messages.get('Messages'):
                documents = [decode_msg(msg['Body']) for msg in received_messages['Messages']]
                logging.info(f'Got {len(documents)} messages from {queue_url_to_name(queue_url)}')
                documents = list(chain(*documents))  # list of lists needs to be flattened

                # await self._storage.store_to_mongo(collection=test_collection', documents=documents)
                # await self._storage.store_to_es(index='test', documents=documents)
                await self._storage.store_to_files(filename_prefix='test', documents=documents)

                await self._sqs.delete_messages(queue_url=queue_url, messages=received_messages)
            else:
                logging.debug(f'Empty queue')
                is_empty_queue = True

    async def run(self):
        queue_urls = await self._sqs.get_queue_list(prefixes=self._config['QUEUE_PREFIX'])
        logging.info(f'run processing queues: {queue_urls_to_names(queue_urls)}')

        queue_processors = []
        for queue_url in queue_urls:
            queue_tags = await self._sqs.get_queue_tags(queue_url=queue_url)
            workers = [self.process_queue(queue_url=queue_url, tags=queue_tags)]*self._config['WORKERS_PER_QUEUE']
            queue_processors.append(asyncio.gather(*workers))

        await asyncio.gather(*queue_processors, return_exceptions=True)

    async def create_test_queues(self):
        with open('resources/example2.json') as file:
            body = f'[{encode_msg(file.read())}]'  # 1 message is json list with N elements
            entries = [{'Id': str(i), 'MessageBody': body} for i in range(10)]

        groups = []
        tags = {
            'routes':  encode_tag({
                'mongo': 'digitalwave',
                'elasticsearch': 'digitalwave',
                's3': 'digitalwave',
                'file': 'digitalwave'
            })}
        for qresp in asyncio.as_completed([self._sqs.create_queue(name=f'Queue{i}', tags=tags) for i in range(0, 2)]):
            queue_url = (await qresp)['QueueUrl']
            logging.info(f'created queue: {queue_url_to_name(queue_url)}')

            group = asyncio.gather(*[self._sqs.send_message_batch(queue_url=queue_url, entries=entries)
                                     for _ in range(10)])
            groups.append(group)

        await asyncio.gather(*groups)
