import logging

from botocore.exceptions import ClientError

from util import queue_urls_to_names, queue_url_to_name


class WrappedSQS:
    def __init__(self, aws_client):
        self._aws_client = aws_client

    async def get_queue_list(self, prefixes: list = None):
        queue_urls = []

        if not prefixes:
            prefixes = ['']

        for prefix in prefixes:
            try:
                response = await self._aws_client.list_queues(QueueNamePrefix=prefix)
            except Exception as error:
                logging.exception(f"Couldn't get queue list with prefix {prefix}")
                raise error
            else:
                if response.get('QueueUrls'):
                    logging.info(f'got queues: {queue_urls_to_names(response["QueueUrls"])}')
                    queue_urls += response['QueueUrls']

        return queue_urls

    async def get_queue_tags(self, queue_url: str) -> dict:
        try:
            response = await self._aws_client.list_queue_tags(QueueUrl=queue_url)
        except Exception as error:
            raise error
        else:
            return response.get('Tags', dict())

    async def receive_messages(self, queue_url: str, batch_size: int) -> dict:
        try:
            messages = await self._aws_client.receive_message(QueueUrl=queue_url,
                                                              AttributeNames=['ALL'],
                                                              WaitTimeSeconds=1,
                                                              MaxNumberOfMessages=batch_size)
        except Exception as error:
            logging.exception(f"Couldn't receive messages from queue: {queue_url}")
            raise error
        else:
            return messages

    async def delete_messages(self, queue_url: str, messages: dict) -> dict:
        entries = [{'Id': str(ind), 'ReceiptHandle': msg['ReceiptHandle']} for ind, msg in
                   enumerate(messages['Messages'])]
        try:
            response = await self._aws_client.delete_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            if response.get('Successful'):
                logging.info(f'Successfully deleted {len(response["Successful"])} from {queue_url_to_name(queue_url)}')
            if response.get('Failed'):
                logging.info(f'Failed when deleting {len(response["Failed"])}  from {queue_url_to_name(queue_url)}')
        except Exception as error:
            logging.exception(f"Couldn't delete messages from queue: {queue_url}")
            raise error
        else:
            return response

    async def create_queue(self, name: str, tags: list) -> dict:
        return await self._aws_client.create_queue(QueueName=name, tags=tags)

    async def is_queue_empty(self, queue_url: str) -> bool:
        try:
            attributes = await self._aws_client.get_queue_attributes(QueueUrl=queue_url,
                                                                     AttributeNames=['ApproximateNumberOfMessages'])
        except Exception as error:
            logging.exception(f"Couldn't check attributes of queue: {queue_url}")
            raise error
        else:
            return int(attributes['Attributes']['ApproximateNumberOfMessages']) == 0

    async def send_message_batch(self, queue_url: str, entries: list):
        return await self._aws_client.send_message_batch(QueueUrl=queue_url, Entries=entries)
