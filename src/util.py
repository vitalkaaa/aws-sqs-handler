import asyncio
import base64
import gzip
import logging


def encode_msg(msg: str):
    return base64.b64encode(gzip.compress(msg.encode('utf-8'))).decode('utf-8')


def decode_msg(msg: str):
    return gzip.decompress(base64.b64decode(msg))


def queue_urls_to_names(queue_urls):
    return [queue_url.split('/')[-1] for queue_url in queue_urls]


def queue_url_to_names(queue_url):
    return queue_url.split('/')[-1]


async def prepare_queue(client):
    with open('example.json') as file:
        body = encode_msg(file.read())
        entries = [{'Id': str(i), 'MessageBody': body} for i in range(10)]

    groups = []
    for qresp in asyncio.as_completed([client.create_queue(QueueName=f'TestQueue{i}') for i in range(10)]):
        queue_url = (await qresp)['QueueUrl']
        logging.info(f'created queue: {queue_url}')

        group = asyncio.gather(*[client.send_message_batch(QueueUrl=queue_url, Entries=entries) for _ in range(10)])
        groups.append(group)

    await asyncio.gather(*groups)
