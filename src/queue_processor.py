import asyncio
import logging
from itertools import chain

from config import Config
from storage import Storage
from util import decode_msg, queue_url_to_name, queue_urls_to_names, encode_msg, encode_tag, decode_tag
from wrapped_sqs import WrappedSQS


class QueuesProcessor:
    def __init__(self, sqs, storage, config):
        self._config: Config = config
        self._storage: Storage = storage
        self._sqs: WrappedSQS = sqs

    async def process_queue(self, queue_url, tags, batch_size=10):
        routes = dict()
        if tags.get('routes'):
            routes = decode_tag(tags['routes'])

        is_empty_queue = False
        while not is_empty_queue:
            received_messages = await self._sqs.receive_messages(queue_url=queue_url, batch_size=batch_size)
            if received_messages.get('Messages'):
                messages = [decode_msg(msg['Body']) for msg in received_messages['Messages']]
                documents = list(chain(*messages))  # list of lists needs to be flattened
                logging.info(f'Got {len(messages)} messages ({len(documents)} documents) '
                             f'from {queue_url_to_name(queue_url)}')
                try:
                    await self._storage.store(routes=routes, documents=documents)
                except Exception as error:
                    raise error
                else:
                    pass
                    # await self._sqs.delete_messages(queue_url=queue_url, messages=received_messages)
            else:
                is_empty_queue = True

    async def run(self):
        WORKERS_PER_QUEUE = self._config['WORKERS_PER_QUEUE']
        queue_urls = await self._sqs.get_queue_list(prefixes=self._config['QUEUE_PREFIX'])

        queue_processors = []
        for queue_url in queue_urls:
            queue_tags = await self._sqs.get_queue_tags(queue_url=queue_url)
            workers = [self.process_queue(queue_url=queue_url, tags=queue_tags) for _ in range(WORKERS_PER_QUEUE)]
            logging.info(f'Running workers for {queue_url_to_name(queue_url)}')
            queue_processors.append(asyncio.gather(*workers))

        results = await asyncio.gather(*queue_processors, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                logging.exception(f'Error when processing {r}')

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
