import asyncio
import functools
import logging
import random
from itertools import chain

from config import Config
from storage import Storage
from util import decode_msg, queue_url_to_name, decode_tag
from wrapped_sqs import WrappedSQS


class QueuesProcessor:
    def __init__(self, sqs: WrappedSQS, storage: Storage, config: Config):
        self._config: Config = config
        self._storage: Storage = storage
        self._sqs: WrappedSQS = sqs
        self.running_workers = dict()

    def _inc_queue_worker_num(self, queue_url):
        queue_name = queue_url_to_name(queue_url)
        self.running_workers[queue_name] = self.running_workers.get(queue_name, 0) + 1

    def _dec_queue_workers_num(self, queue_url):
        queue_name = queue_url_to_name(queue_url)
        self.running_workers[queue_name] = self.running_workers.get(queue_name, 0) - 1
        if self.running_workers[queue_name] == 0:
            self.running_workers.pop(queue_name)

    def _handle_task_result(self, queue_url: str, task: asyncio.Task) -> None:
        try:
            task.result()
        except Exception as error:
            logging.exception(f'Error in {task.get_name()} worker: {error}', exc_info=False)
        finally:
            self._dec_queue_workers_num(queue_url)

    async def process_queue(self, queue_url: str, routes: dict, batch_size: int = 10) -> None:
        self._inc_queue_worker_num(queue_url)

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

    async def run(self) -> None:
        WORKERS_PER_QUEUE = self._config['WORKERS_PER_QUEUE']
        queue_urls = await self._sqs.get_queue_list(prefixes=self._config['QUEUE_PREFIX'])

        for queue_url in queue_urls:
            if queue_url_to_name(queue_url) in self.running_workers or await self._sqs.is_queue_empty(queue_url):
                continue

            routes = dict()
            queue_tags = await self._sqs.get_queue_tags(queue_url=queue_url)
            if queue_tags.get('routes'):
                routes = decode_tag(queue_tags['routes'])
            logging.info(f'Routes for {queue_url_to_name(queue_url)}: {routes}')

            for i in range(WORKERS_PER_QUEUE):
                task_name = f'{queue_url_to_name(queue_url)}-{i}'
                task = asyncio.create_task(self.process_queue(queue_url=queue_url, routes=routes))
                task.set_name(task_name)
                task.add_done_callback(functools.partial(self._handle_task_result, queue_url))
