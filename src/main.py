import asyncio
import json
import logging

import aiobotocore
import motor.motor_asyncio

from wrapped_sqs import receive_messages, get_queue_list, delete_messages
from util import decode_msg, queue_urls_to_names, prepare_queue, queue_url_to_names

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(funcName)s(%(lineno)d): %(message)s')
with open('config.json') as file:
    config = json.loads(file.read())

WORKERS_PER_QUEUE = config['WORKERS_PER_QUEUE']
TIMEOUT_BETWEEN_QUEUE_CHECKING = config['TIMEOUT_BETWEEN_QUEUE_CHECKING']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_REGION_NAME = config['AWS']['AWS_REGION_NAME']


async def store_to_mongo(mongo_client, collection, documents):
    db = mongo_client.test_database
    col = db[collection]
    logging.info(f'Stored to mongo {len(documents)} documents in {collection}')
    await col.insert_many(documents)


async def process_queue(aws_client, queue_url, mongo_client, batch_size=10):
    is_empty_queue = False
    while not is_empty_queue:
        received_messages = await receive_messages(aws_client=aws_client, queue_url=queue_url, batch_size=batch_size)
        if received_messages.get('Messages'):
            logging.info(f'Got {len(received_messages["Messages"])} messages from {queue_url_to_names(queue_url)}')
            documents = [json.loads(decode_msg(msg['Body'])) for msg in received_messages['Messages']]
            await store_to_mongo(mongo_client=mongo_client, collection='test_collection', documents=documents)
            await delete_messages(aws_client=aws_client, queue_url=queue_url, messages=received_messages)
        else:
            logging.debug(f'Empty queue')
            is_empty_queue = True


async def run_queues_processing(aws_client, mongo_client):
    queue_urls = await get_queue_list(aws_client=aws_client, prefixes=[''])
    logging.info(f'run processing queues: {queue_urls_to_names(queue_urls)}')

    queue_processors = [
        asyncio.gather(*[process_queue(aws_client=aws_client, queue_url=queue_url, mongo_client=mongo_client)
                         for _ in range(WORKERS_PER_QUEUE)])
        for queue_url in queue_urls
    ]
    await asyncio.gather(*queue_processors)


async def main():
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017')

    async with aiobotocore.get_session().create_client(service_name='sqs',
                                                       region_name=AWS_REGION_NAME,
                                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                                       aws_access_key_id=AWS_ACCESS_KEY_ID) as aws_client:
        await prepare_queue(client=aws_client)

        while True:
            asyncio.create_task(run_queues_processing(aws_client=aws_client, mongo_client=mongo_client))
            await asyncio.sleep(TIMEOUT_BETWEEN_QUEUE_CHECKING)


if __name__ == '__main__':
    asyncio.run(main())
