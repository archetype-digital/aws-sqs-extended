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

import hashlib
import json

import pytest


@pytest.fixture
def send_message_extended_resource(sqs_extended_message, sqs_resource_queue):
    attributes = {'send_message': sqs_resource_queue.send_message}

    add_custom_method = sqs_extended_message.add_send_message_extended(None)
    add_custom_method(class_attributes=attributes)
    return attributes['send_message_extended']


@pytest.fixture
def receive_message_extended_resource(
        sqs_extended_message, sqs_resource_queue):
    attributes = {'receive_messages': sqs_resource_queue.receive_messages}

    add_custom_method = sqs_extended_message.add_receive_message_extended(
        'creating-resource-class.sqs.Queue')
    add_custom_method(class_attributes=attributes)
    return attributes['receive_messages_extended']


@pytest.fixture
def delete_message_extended_resource(session, sqs_extended_message):
    session.events.register(
        'creating-resource-class.sqs.Message',
        sqs_extended_message.add_delete_message_extended(
            'creating-resource-class.sqs.Message')
    )
    yield


@pytest.fixture
def send_message_batch_extended_resource(
        sqs_extended_message, sqs_resource_queue):
    attributes = {'send_messages': sqs_resource_queue.send_messages}

    add_custom_method = sqs_extended_message.add_send_message_batch_extended(
        'creating-resource-class.sqs.Queue')
    add_custom_method(class_attributes=attributes)
    return attributes['send_messages_extended']


@pytest.fixture
def delete_message_batch_extended_resource(
        sqs_extended_message, sqs_resource_queue):
    attributes = {'delete_messages': sqs_resource_queue.delete_messages}

    add_custom_method = sqs_extended_message.add_delete_message_batch_extended(
        'creating-resource-class.sqs.Queue')
    add_custom_method(class_attributes=attributes)
    return attributes['delete_messages_extended']


##
# error handling tests are done by _client tests
##

##
# simple send/receive/delete test
##


def test_extended_messaging_w_small_text(
        sqs_resource_queue, send_message_extended_resource,
        receive_message_extended_resource, delete_message_extended_resource):
    body = '{"message": "small text"}'
    attr = {
        'string_attr': {
            'StringValue': 'string_something',
            'DataType': 'String'
        },
        'binary_attr': {
            'BinaryValue': b'bytes_something',
            'DataType': 'Binary'
        },
    }

    # send
    res = send_message_extended_resource(
        MessageBody=body, MessageAttributes=attr)
    assert 'MessageId' in res

    # receive
    res = sqs_resource_queue.receive_messages(
        MessageAttributeNames=['All'], VisibilityTimeout=0, WaitTimeSeconds=0)
    messages = receive_message_extended_resource(
        MessageAttributeNames=['All'])

    assert isinstance(messages, list)
    message = messages[0]
    assert message.meta.data['MessageId'] == res[0].meta.data['MessageId']
    assert message.meta.data['MD5OfBody'] == res[0].meta.data['MD5OfBody']
    assert message.body == body
    assert message.meta.data['MD5OfBody'] == hashlib.md5(
        body.encode()).hexdigest()
    assert message.meta.data['MD5OfMessageAttributes'] == (
        res[0].meta.data['MD5OfMessageAttributes'])

    # delete
    message.delete_extended()
    res = sqs_resource_queue.receive_messages(WaitTimeSeconds=0)
    assert res == []


##
# large send/receive/delete test
##

def test_extended_messaging_w_large_text(
        s3_bucket, bucket_name, big_message, sqs_resource_queue,
        sqs_extended_message, s3_client,
        send_message_extended_resource, receive_message_extended_resource,
        delete_message_extended_resource):
    body = big_message
    attr = {
        'string_attr': {
            'StringValue': 'string_something',
            'DataType': 'String'
        },
        'binary_attr': {
            'BinaryValue': b'bytes_something',
            'DataType': 'Binary'
        },
    }

    # send
    res = send_message_extended_resource(
        MessageBody=body, MessageAttributes=attr)
    assert 'MessageId' in res

    # receive
    res = sqs_resource_queue.receive_messages(
        MessageAttributeNames=['All'], VisibilityTimeout=0, WaitTimeSeconds=0)
    message_body = json.loads(res[0].body)

    assert isinstance(res, list)
    assert len(res) == 1
    assert ('s3BucketName' in message_body and 's3Key' in message_body)
    assert message_body['s3BucketName'] == bucket_name

    messages = receive_message_extended_resource(
        MessageAttributeNames=['All'])

    assert isinstance(messages, list)
    message = messages[0]
    assert message.body == body
    assert message.meta.data['MD5OfBody'] == hashlib.md5(
        body.encode()).hexdigest()

    # delete
    message.delete_extended()
    res = sqs_resource_queue.receive_messages(WaitTimeSeconds=0)
    assert res == []
    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['KeyCount'] == 0

##
# large multiple send/receive/delete test
##


def test_multiple_extended_messaging_w_large_text(
        s3_bucket, s3_client, bucket_name, big_message, sqs_resource_queue,
        send_message_batch_extended_resource,
        receive_message_extended_resource,
        delete_message_batch_extended_resource):
    body1 = big_message
    body2 = json.loads(big_message)
    body2['id'] = 'TEST_BIG_MESSAGE2'
    body2 = json.dumps(body2)

    # send
    res = send_message_batch_extended_resource(
        Entries=[
            {'Id': '1', 'MessageBody': body1},
            {'Id': '2', 'MessageBody': body2}])

    assert 'Successful' in res
    assert len(res['Successful']) == 2
    assert list(map(lambda x: x['Id'], res['Successful'])) == ['1', '2']

    # receive
    messages = receive_message_extended_resource(
        MessageAttributeNames=['All'], MaxNumberOfMessages=10)
    assert isinstance(messages, list)
    assert len(messages) == 2

    # delete
    # for resource-based multiple deletion,
    # must use .meta.data['ReceiptHandle'] instead of .receipt_handle
    # otherwise, S3 message will be remained.
    # rhs = list(map(lambda x: x.receipt_handle, messages))
    rhmeta = list(map(lambda x: x.meta.data['ReceiptHandle'], messages))
    delete_message_batch_extended_resource(
        Entries=[
            {'Id': '1', 'ReceiptHandle': rhmeta[0]},
            {'Id': '2', 'ReceiptHandle': rhmeta[1]},
        ])
    res = sqs_resource_queue.receive_messages(WaitTimeSeconds=1)
    assert res == []

    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['KeyCount'] == 0
