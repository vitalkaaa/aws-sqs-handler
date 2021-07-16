import asyncio
import functools
import logging
from itertools import chain

from config import Config
from storage import Storage
from util import decode_msg, queue_url_to_name, decode_tag
from wrapped_sqs import WrappedSQS


class QueuesProcessor:
    def __init__(self, sqs, storage, config):
        self._config: Config = config
        self._storage: Storage = storage
        self._sqs: WrappedSQS = sqs
        self.running_queues = set()

    def _handle_task_result(self, queue_url, task: asyncio.Task):
        try:
            task.result()
        except Exception as error:
            logging.exception(f'Error in {task.get_name()} worker: {error}', exc_info=False)
        finally:
            self.running_queues.discard(queue_url)

    async def process_queue(self, queue_url, tags, batch_size=10):
        self.running_queues.add(queue_url)
        routes = dict()
        if tags.get('routes'):
            routes = decode_tag(tags['routes'])

        while True:
            received_messages = await self._sqs.receive_messages(queue_url=queue_url, batch_size=batch_size)
            if received_messages.get('Messages'):
                messages = [decode_msg(msg['Body']) for msg in received_messages['Messages']]
                documents = list(chain(*messages))  # list of lists needs to be flattened
                logging.info(f'Got {len(messages)} messages ({len(documents)} documents) '
                             f'from {queue_url_to_name(queue_url)}')

                await self._storage.store(routes=routes, documents=documents)
                await self._sqs.delete_messages(queue_url=queue_url, messages=received_messages)
            else:
                break

    async def run(self):
        WORKERS_PER_QUEUE = self._config['WORKERS_PER_QUEUE']
        queue_urls = await self._sqs.get_queue_list(prefixes=self._config['QUEUE_PREFIX'])

        for queue_url in queue_urls:
            if queue_url in self.running_queues or await self._sqs.is_queue_empty(queue_url):
                continue

            queue_tags = await self._sqs.get_queue_tags(queue_url=queue_url)

            for i in range(WORKERS_PER_QUEUE):
                task_name = f'{queue_url_to_name(queue_url)}-{i}'
                task = asyncio.create_task(self.process_queue(queue_url=queue_url, tags=queue_tags))
                task.set_name(task_name)
                task.add_done_callback(functools.partial(self._handle_task_result, queue_url))
