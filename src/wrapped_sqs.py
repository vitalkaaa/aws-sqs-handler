import logging

from botocore.exceptions import ClientError

from src.util import queue_urls_to_names, queue_url_to_names


async def get_queue_list(aws_client, prefixes=None):
    queue_urls = []

    if not prefixes:
        prefixes = ['']

    for prefix in prefixes:
        try:
            response = await aws_client.list_queues(QueueNamePrefix=prefix)
        except ClientError as error:
            logging.exception(f"Couldn't get queue list with prefix {prefix}")
            raise error
        else:
            if response.get('QueueUrls'):
                logging.info(f'got queues: {queue_urls_to_names(response["QueueUrls"])}')
                queue_urls += response['QueueUrls']

    return queue_urls


async def receive_messages(aws_client, queue_url, batch_size):
    try:
        messages = await aws_client.receive_message(QueueUrl=queue_url,
                                                    AttributeNames=['ALL'],
                                                    WaitTimeSeconds=1,
                                                    MaxNumberOfMessages=batch_size)
    except ClientError as error:
        logging.exception(f"Couldn't receive messages from queue: {queue_url}")
        raise error
    else:
        return messages


async def delete_messages(aws_client, queue_url, messages):
    entries = [{'Id': str(ind), 'ReceiptHandle': msg['ReceiptHandle']} for ind, msg in enumerate(messages['Messages'])]
    try:
        response = await aws_client.delete_message_batch(
            QueueUrl=queue_url,
            Entries=entries
        )
        if response.get('Successful'):
            logging.info(f'Successfully deleted {len(response["Successful"])} from {queue_url_to_names(queue_url)}')
        if response.get('Failed'):
            logging.info(f'Failed when deleting {len(response["Failed"])}  from {queue_url_to_names(queue_url)}')
    except ClientError as error:
        logging.exception(f"Couldn't delete messages from queue: {queue_url}")
        raise error
    else:
        return response
