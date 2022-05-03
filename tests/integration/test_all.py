"""
The MIT License (MIT)

Copyright (c) 2021 Archetype Digital Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
import sys
import time
import uuid

import boto3

sys.path.append('./')
import aws_sqs_ext_client  # noqa: F401, E402

INTEGRATION_TEST_QUEUE_NAME = str(uuid.uuid4())  # 'test-sqs-queue-123456789'
INTEGRATION_TEST_BUCKET_NAME = str(uuid.uuid4())  # 'test-sqs-bucket-123456789'


def mkdata():
    message = {'id': 'TEST_BIG_MESSAGE'}
    for _ in range(0, 3500):
        message[str(uuid.uuid4())] = str(uuid.uuid4())

    return message


def cleanup():
    s3 = boto3.client('s3')
    s3.delete_bucket(Bucket=INTEGRATION_TEST_BUCKET_NAME)

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=INTEGRATION_TEST_QUEUE_NAME)
    queue.delete()
    queue = sqs.get_queue_by_name(
        QueueName=f'{INTEGRATION_TEST_QUEUE_NAME}.fifo')
    queue.delete()


def small_data_messaging_as_large_data_with_resource():
    print('=========================================================')
    print('test/example to send small message dealt as large message')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME, always_through_s3=True)

    body = '{"message": "small text"}'

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_message_extended(MessageBody=body)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


def custom_threshold_with_resource():
    print('=========================================================')
    print('test/example to configure the threshold')
    session = boto3.session.Session()
    session.extend_sqs(
        INTEGRATION_TEST_BUCKET_NAME, message_size_threshold=2**4)

    body = '{"message": "small text"}'

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_message_extended(MessageBody=body)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


def default_session_with_resource():
    print('=========================================================')
    print('test/example to use default session instead of creating new')
    session = boto3.DEFAULT_SESSION
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME)

    body = '{"message": "small text"}'

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_message_extended(MessageBody=body)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


def large_data_messaging_with_resource():
    print('=========================================================')
    print('test/example to send large message')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME)

    body = json.dumps(mkdata())

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_message_extended(MessageBody=body)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


def multiple_large_data_messaging_with_resource():
    print('=========================================================')
    print('test/example to send multiple small message dealt as large message')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME, always_through_s3=True)

    bodies = [{
        'Id': '1', 'MessageBody': "{'message': 'small message 1'}",
    }, {
        'Id': '2', 'MessageBody': "{'message': 'small message 2'}",
    }]

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_messages_extended(Entries=bodies)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    receipt_handles = []
    for i, r in enumerate(res):
        print(f'got a message, beginning of which is {r.body[0:100]}')
        receipt_handles.append({
            'Id': str(i), 'ReceiptHandle': r.meta.data['ReceiptHandle']})

    # bulk deletion
    # NOTE that if you use delete_messages_extended requires receipt handles
    # from sqs.Message.meta.data['ReceiptHandle']
    # instead of sqs.Message.receipt_handle that refers the message without
    # S3 association
    if receipt_handles:
        res = queue.delete_messages_extended(Entries=receipt_handles)


def large_data_messaging_with_client():
    print('=========================================================')
    print('test/example to send large message by client API')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME)

    body = json.dumps(mkdata())

    sqs = session.client('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = sqs.send_message_extended(
        QueueUrl=queue['QueueUrl'], MessageBody=body)

    time.sleep(2)
    res = sqs.receive_message_extended(
        QueueUrl=queue['QueueUrl'],  MessageAttributeNames=['All'],
        MaxNumberOfMessages=10, WaitTimeSeconds=5)

    received = res['Messages'] if 'Messages' in res else []
    for r in received:
        print(f'got a message, beginning of which is {r["Body"][0:100]}')
        sqs.delete_message_extended(
            QueueUrl=queue['QueueUrl'], ReceiptHandle=r['ReceiptHandle'])


def multiple_large_data_messaging_with_client():
    print('=========================================================')
    print('test/example to send multiple messages via S3 by client API')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME, always_through_s3=True)

    bodies = [{
        'Id': '1', 'MessageBody': "{'message': 'small message 1'}",
    }, {
        'Id': '2', 'MessageBody': "{'message': 'small message 2'}",
    }]

    sqs = session.client('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = sqs.send_message_batch_extended(
        QueueUrl=queue['QueueUrl'], Entries=bodies)

    time.sleep(2)
    res = sqs.receive_message_extended(
        QueueUrl=queue['QueueUrl'], MessageAttributeNames=['All'],
        MaxNumberOfMessages=10, WaitTimeSeconds=5)

    receipt_handles = []
    received = res['Messages'] if 'Messages' in res else []
    for i, r in enumerate(received):
        print(f'got a message, beginning of which is {r["Body"][0:100]}')
        receipt_handles.append({
            'Id': str(i), 'ReceiptHandle': r['ReceiptHandle']})

    # bulk deletion
    if receipt_handles:
        res = sqs.delete_message_batch_extended(
            QueueUrl=queue['QueueUrl'], Entries=receipt_handles)


def small_data_messaging_as_large_data_with_resource_to_fifo():
    print('=========================================================')
    print('test/example to send small message dealt as large message to FIFO')
    session = boto3.session.Session()
    session.extend_sqs(INTEGRATION_TEST_BUCKET_NAME, always_through_s3=True)

    body = '{"message": "small text"}'
    messageGroupId = str(uuid.uuid4())
    messageDeduplicationId = str(uuid.uuid4())

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=f'{INTEGRATION_TEST_QUEUE_NAME}.fifo',
        Attributes={'FifoQueue': 'true', 'DelaySeconds': '5'})

    res = queue.send_message_extended(
        MessageBody=body, MessageGroupId=messageGroupId,
        MessageDeduplicationId=messageDeduplicationId)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


def small_data_messaging_as_large_data_with_bucket_creation():
    print('=========================================================')
    print('test/example to send small message without s3 bucket creation')
    print('s3 bucket must be created by other tests before this')
    session = boto3.session.Session()
    session.extend_sqs(
        INTEGRATION_TEST_BUCKET_NAME, always_through_s3=True,
        s3_bucket_params=None)

    body = '{"message": "small text"}'

    sqs = session.resource('sqs')
    queue = sqs.create_queue(
        QueueName=INTEGRATION_TEST_QUEUE_NAME,
        Attributes={'DelaySeconds': '5'})

    res = queue.send_message_extended(MessageBody=body)

    time.sleep(2)
    res = queue.receive_messages_extended(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10,
        WaitTimeSeconds=5)

    for r in res:
        print(f'got a message, beginning of which is {r.body[0:100]}')
        r.delete_extended()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    # multiple function in sequence verifies there are no issue to invoke
    # session creation.
    small_data_messaging_as_large_data_with_resource()
    custom_threshold_with_resource()
    default_session_with_resource()
    large_data_messaging_with_resource()
    multiple_large_data_messaging_with_client()
    large_data_messaging_with_client()
    multiple_large_data_messaging_with_client()
    small_data_messaging_as_large_data_with_resource_to_fifo()
    small_data_messaging_as_large_data_with_bucket_creation()

    cleanup()
