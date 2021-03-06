import argparse
import asyncio
import logging

import aiobotocore

from config import Config
from queue_processor import QueuesProcessor
from util import queue_urls_to_names
from storage import Storage
from wrapped_sqs import WrappedSQS

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str, help="Config file")
args = parser.parse_args()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(funcName)s(%(lineno)d): %(message)s')
config = Config(args.config)


async def main():
    TIMEOUT_BETWEEN_QUEUE_CHECKING = config['TIMEOUT_BETWEEN_QUEUE_CHECKING']
    AWS_SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
    AWS_ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID']
    AWS_REGION_NAME = config['AWS']['REGION_NAME']

    async with aiobotocore.get_session().create_client(service_name='sqs',
                                                       region_name=AWS_REGION_NAME,
                                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                                       aws_access_key_id=AWS_ACCESS_KEY_ID) as aws_client:

        sqs = WrappedSQS(aws_client=aws_client)
        storage = Storage(config)
        queue_processor = QueuesProcessor(sqs=sqs, storage=storage, config=config)

        while True:
            running_tasks = [task for task in asyncio.all_tasks() if not task.done()]
            done_tasks = [task for task in asyncio.all_tasks() if task.done()]

            logging.info(f'Running tasks: {len(running_tasks)}, Done tasks: {len(done_tasks)}')
            logging.info(f'Workers in process: {queue_processor.running_workers}')
            asyncio.create_task(queue_processor.run())
            await asyncio.sleep(TIMEOUT_BETWEEN_QUEUE_CHECKING)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
