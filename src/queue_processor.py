import asyncio
import json
import logging

from src.util import decode_msg, queue_url_to_name, queue_urls_to_names, encode_msg


class QueuesProcessor:
    def __init__(self, sqs, storage, config):
        self._config = config
        self._storage = storage
        self._sqs = sqs

    async def process_queue(self, queue_url, batch_size=10):
        is_empty_queue = False
        while not is_empty_queue:
            received_messages = await self._sqs.receive_messages(queue_url=queue_url, batch_size=batch_size)
            if received_messages.get('Messages'):
                logging.info(f'Got {len(received_messages["Messages"])} messages from {queue_url_to_name(queue_url)}')
                documents = [json.loads(decode_msg(msg['Body'])) for msg in received_messages['Messages']]

                await self._storage.store_to_mongo(collection='test_collection', documents=documents)

                # await self._sqs.delete_messages(queue_url=queue_url, messages=received_messages)
            else:
                logging.debug(f'Empty queue')
                is_empty_queue = True

    async def run(self):
        queue_urls = await self._sqs.get_queue_list(prefixes=self._config['QUEUE_PREFIX'])
        logging.info(f'run processing queues: {queue_urls_to_names(queue_urls)}')

        queue_processors = [
            asyncio.gather(*[self.process_queue(queue_url=queue_url)
                             for _ in range(self._config['WORKERS_PER_QUEUE'])])
            for queue_url in queue_urls
        ]
        await asyncio.gather(*queue_processors)

    async def create_test_queues(self):
        with open('example.json') as file:
            body = encode_msg(file.read())
            entries = [{'Id': str(i), 'MessageBody': body} for i in range(10)]

        groups = []
        for qresp in asyncio.as_completed([self._sqs.create_queue(name=f'TestQueue{i}') for i in range(10)]):
            queue_url = (await qresp)['QueueUrl']
            logging.info(f'created queue: {queue_url_to_name(queue_url)}')

            group = asyncio.gather(*[self._sqs.send_message_batch(queue_url=queue_url, entries=entries)
                                     for _ in range(10)])
            groups.append(group)

        await asyncio.gather(*groups)
