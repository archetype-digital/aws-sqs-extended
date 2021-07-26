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

import botocore
import pytest
from aws_sqs_ext_client.extended_messaging import SQSExtendedMessage


@pytest.fixture
def send_message_extended_client(sqs_extended_message, sqs_client):
    attributes = {'send_message': sqs_client.send_message}

    add_custom_method = sqs_extended_message.add_send_message_extended(None)
    add_custom_method(class_attributes=attributes)
    return attributes['send_message_extended']


@pytest.fixture
def receive_message_extended_client(sqs_extended_message, sqs_client):
    attributes = {'receive_message': sqs_client.receive_message}

    add_custom_method = sqs_extended_message.add_receive_message_extended(
        'creating-client-class.sqs')
    add_custom_method(class_attributes=attributes)
    return attributes['receive_message_extended']


@pytest.fixture
def delete_message_extended_client(sqs_extended_message, sqs_client):
    attributes = {'delete_message': sqs_client.delete_message}

    add_custom_method = sqs_extended_message.add_delete_message_extended(
        'creating-client-class.sqs')
    add_custom_method(class_attributes=attributes)
    return attributes['delete_message_extended']


@pytest.fixture
def send_message_batch_extended_client(sqs_extended_message, sqs_client):
    attributes = {'send_message_batch': sqs_client.send_message_batch}

    add_custom_method = sqs_extended_message.add_send_message_batch_extended(
        'creating-client-class.sqs')
    add_custom_method(class_attributes=attributes)
    return attributes['send_message_batch_extended']


@pytest.fixture
def delete_message_batch_extended_client(sqs_extended_message, sqs_client):
    attributes = {'delete_message_batch': sqs_client.delete_message_batch}

    add_custom_method = sqs_extended_message.add_delete_message_batch_extended(
        'creating-client-class.sqs')
    add_custom_method(class_attributes=attributes)
    return attributes['delete_message_batch_extended']


def test_add_custom_method(sqs_extended_message):
    def f(**kwargs):
        return True

    tests = [{
        'given': {
            'attributes': {'send_message': f},
            'callee': sqs_extended_message.add_send_message_extended,
            'args': ('creating-client-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'send_message_extended' in x,
            'registration': (
                lambda x: x['send_message_extended'].__name__ ==
                'send_message_extended')
        }
    }, {
        'given': {
            'attributes': {'send_message': f},
            'callee': sqs_extended_message.add_send_message_extended,
            'args': ('creating-resource-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'send_message_extended' in x,
            'registration': (
                lambda x: x['send_message_extended'].__name__ ==
                'send_message_extended')
        }
    }, {
        'given': {
            'attributes': {'receive_message': f},
            'callee': sqs_extended_message.add_receive_message_extended,
            'args': ('creating-client-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'receive_message_extended' in x,
            'registration': (
                lambda x: x['receive_message_extended'].__name__ ==
                'receive_message_extended')
        }
    }, {
        'given': {
            'attributes': {'receive_messages': f},
            'callee': sqs_extended_message.add_receive_message_extended,
            'args': ('creating-resource-class.sqs.Queue'),
        },
        'wants': {
            'in_attributes': lambda x: 'receive_messages_extended' in x,
            'registration': (
                lambda x: x['receive_messages_extended'].__name__ ==
                'receive_message_extended')
        }
    }, {
        'given': {
            'attributes': {'delete_message': f},
            'callee': sqs_extended_message.add_delete_message_extended,
            'args': ('creating-client-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'delete_message_extended' in x,
            'registration': (
                lambda x: x['delete_message_extended'].__name__ ==
                'delete_message_extended')
        }
    }, {
        'given': {
            'attributes': {'delete': f},
            'callee': sqs_extended_message.add_delete_message_extended,
            'args': ('creating-resource-class.sqs.Message'),
        },
        'wants': {
            'in_attributes': lambda x: 'delete_extended' in x,
            'registration': (
                lambda x: x['delete_extended'].__name__ ==
                'delete_message_extended')
        }
    }, {
        'given': {
            'attributes': {'send_message_batch': f},
            'callee': sqs_extended_message.add_send_message_batch_extended,
            'args': ('creating-client-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'send_message_batch_extended' in x,
            'registration': (
                lambda x: x['send_message_batch_extended'].__name__ ==
                'send_message_batch_extended')
        }
    }, {
        'given': {
            'attributes': {'send_messages': f},
            'callee': sqs_extended_message.add_send_message_batch_extended,
            'args': ('creating-resource-class.sqs.Queue'),
        },
        'wants': {
            'in_attributes': lambda x: 'send_messages_extended' in x,
            'registration': (
                lambda x: x['send_messages_extended'].__name__ ==
                'send_message_batch_extended')
        }
    }, {
        'given': {
            'attributes': {'delete_message_batch': f},
            'callee': sqs_extended_message.add_delete_message_batch_extended,
            'args': ('creating-client-class.sqs'),
        },
        'wants': {
            'in_attributes': lambda x: 'delete_message_batch_extended' in x,
            'registration': (
                lambda x: x['delete_message_batch_extended'].__name__ ==
                'delete_message_batch_extended')
        }
    }, {
        'given': {
            'attributes': {'delete_messages': f},
            'callee': sqs_extended_message.add_delete_message_batch_extended,
            'args': ('creating-resource-class.sqs.Queue'),
        },
        'wants': {
            'in_attributes': lambda x: 'delete_messages_extended' in x,
            'registration': (
                lambda x: x['delete_messages_extended'].__name__ ==
                'delete_message_batch_extended')
        }
    }]

    for t in tests:
        attributes = t['given']['attributes']
        add_custom_method = t['given']['callee'](t['given']['args'])
        add_custom_method(class_attributes=attributes)

        assert t['wants']['in_attributes'](attributes)
        assert t['wants']['registration'](attributes)

##
# send_message_extended error handling test
##


def test_send_message_extended_wo_params(send_message_extended_client):
    with pytest.raises(ValueError) as excinfo:
        send_message_extended_client()
    assert "message body is required" in str(excinfo.value)


def test_send_message_extended_w_invalid_attribute(
        send_message_extended_client):
    with pytest.raises(ValueError) as excinfo:
        send_message_extended_client(
            MessageAttributes={'ExtendedPayloadSize': 'test'}
        )
    assert "ExtendedPayloadSize is reserved name" in str(excinfo.value)


def test_send_message_extended_wo_required_param_for_original(
        send_message_extended_client):
    with pytest.raises(
            botocore.exceptions.ParamValidationError) as excinfo:
        send_message_extended_client(MessageBody='{"message": "small text"}')

    assert 'Missing required parameter in input: "QueueUrl"' in str(
        excinfo.value)


##
# receive_message_extended error handling test
##


def test_receive_message_extended_w_invalid_attr(
        receive_message_extended_client):
    with pytest.raises(ValueError) as excinfo:
        receive_message_extended_client(AttributeNames={})
    assert "AttributeNames or MessageAttributeNames must be list" in str(
        excinfo.value)


def test_receive_message_extended_w_invalid_mattr(
        receive_message_extended_client):
    with pytest.raises(ValueError) as excinfo:
        receive_message_extended_client(MessageAttributeNames={})
    assert "AttributeNames or MessageAttributeNames must be list" in str(
        excinfo.value)


def test_receive_message_extended_wo_required_param_for_original(
        receive_message_extended_client):
    with pytest.raises(
            botocore.exceptions.ParamValidationError) as excinfo:
        receive_message_extended_client()

    assert 'Missing required parameter in input: "QueueUrl"' in str(
        excinfo.value)


def test_receive_message_extended_wo_response(
        receive_message_extended_client, sqs_client_queue):
    res = receive_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        MessageAttributeNames=[])
    assert 'ResponseMetadata' in res
    assert 'Messages' not in res

##
# delete_message_extended error handling test
##


def test_delete_message_extended_wo_params(
        delete_message_extended_client):
    with pytest.raises(ValueError) as excinfo:
        delete_message_extended_client()
    assert "invalid call without ReceiptHandle" in str(excinfo.value)


##
# send_message_batch_extended error handling test
##

def test_send_message_batch_extended_wo_params(
        send_message_batch_extended_client):
    with pytest.raises(ValueError) as excinfo:
        send_message_batch_extended_client()
    assert "Entries (list) must be given" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        send_message_batch_extended_client(Entries={'Messages': {}})
    assert "Entries (list) must be given" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        send_message_batch_extended_client(Entries=[{}])
    assert (
        "message body is required, found in 0" in str(excinfo.value))

    with pytest.raises(ValueError) as excinfo:
        send_message_batch_extended_client(Entries=[
            {'MessageAttributes': {
                'ExtendedPayloadSize': {'key': 'value'}
            }}
        ])
    assert (
        "ExtendedPayloadSize is reserved name, found in 0"
        in str(excinfo.value))

##
# delete_message_batch_extended error handling test
##


def test_delete_message_batch_extended_wo_params(
        delete_message_batch_extended_client):
    with pytest.raises(ValueError) as excinfo:
        delete_message_batch_extended_client()
    assert "Entries (list) must be given" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        delete_message_batch_extended_client(Entries={'Messages': {}})
    assert "Entries (list) must be given" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        delete_message_batch_extended_client(Entries=[{}])
    assert (
        "missing ReceiptHandle, found 0" in str(excinfo.value))


# ##
# # simple send/receive/delete test
# ##


def test_extended_messaging_w_small_text(
        send_message_extended_client, receive_message_extended_client,
        delete_message_extended_client,
        sqs_client_queue, sqs_client):
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
    res = send_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageBody=body,
        MessageAttributes=attr)
    assert 'MessageId' in res

    # receive
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'],
        VisibilityTimeout=0, WaitTimeSeconds=0)
    res_extended = receive_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'])

    assert res.keys() == res_extended.keys()
    assert res_extended['Messages'][0]['MessageId'] == (
        res['Messages'][0]['MessageId'])
    assert res_extended['Messages'][0]['Body'] == body
    assert res_extended['Messages'][0]['MD5OfBody'] == (
        res['Messages'][0]['MD5OfBody'])
    assert res_extended['Messages'][0]['MD5OfBody'] == hashlib.md5(
        body.encode()).hexdigest()
    assert res_extended['Messages'][0]['MD5OfMessageAttributes'] == (
        res['Messages'][0]['MD5OfMessageAttributes'])

    # delete
    delete_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        ReceiptHandle=res_extended['Messages'][0]['ReceiptHandle'])
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], WaitTimeSeconds=0)
    assert 'Messages' not in res


# ##
# # large send/receive/delete test
# ##

def test_extended_messaging_w_large_text(
        s3_bucket, sqs_client_queue, sqs_client, s3_client, big_message,
        bucket_name,
        send_message_extended_client, receive_message_extended_client,
        delete_message_extended_client):
    body = big_message

    # send
    res = send_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        MessageBody=body)

    assert 'MessageId' in res

    # receive temporal message
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'],
        VisibilityTimeout=0, WaitTimeSeconds=0)
    assert 'Messages' in res
    assert len(res['Messages']) == 1
    assert (
        's3BucketName' in res['Messages'][0]['Body'] and
        's3Key' in res['Messages'][0]['Body'])
    assert json.loads(
        res['Messages'][0]['Body'])['s3BucketName'] == bucket_name
    assert 'MD5OfBody' in res['Messages'][0]
    assert 'MD5OfMessageAttributes' in res['Messages'][0]

    # receive actual message
    res = receive_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'])

    assert 'Messages' in res
    assert len(res['Messages']) == 1
    assert res['Messages'][0]['Body'] == body
    assert res['Messages'][0]['MD5OfBody'] == hashlib.md5(
        body.encode()).hexdigest()
    assert 'MD5OfMessageAttributes' not in res['Messages'][0]

    # delete
    delete_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        ReceiptHandle=res['Messages'][0]['ReceiptHandle'])
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], WaitTimeSeconds=1)
    assert 'Messages' not in res

    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['KeyCount'] == 0


def test_always_extended_messaging(
        s3_bucket, session, bucket_name, sqs_client, sqs_client_queue,
        delete_message_extended_client):
    sqs = SQSExtendedMessage(session, bucket_name, always_through_s3=True)
    attributes = {'send_message': sqs_client.send_message}
    add_custom_method = sqs.add_send_message_extended(None)
    add_custom_method(class_attributes=attributes)
    send_method = attributes['send_message_extended']
    body = '{"message": "small text"}'

    # send
    res = send_method(QueueUrl=sqs_client_queue['QueueUrl'], MessageBody=body)
    assert 'MessageId' in res

    # receive
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'])
    assert 'Messages' in res
    assert len(res['Messages']) == 1
    assert (
        's3BucketName' in res['Messages'][0]['Body'] and
        's3Key' in res['Messages'][0]['Body'])
    assert json.loads(
        res['Messages'][0]['Body'])['s3BucketName'] == bucket_name

    # delete
    delete_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        ReceiptHandle=res['Messages'][0]['ReceiptHandle'])


def test_extended_messaging_w_lower_threshold(
        s3_bucket, session, bucket_name, sqs_client, sqs_client_queue,
        delete_message_extended_client):
    sqs = SQSExtendedMessage(session, bucket_name, message_size_threshold=10)
    attributes = {'send_message': sqs_client.send_message}
    add_custom_method = sqs.add_send_message_extended(None)
    add_custom_method(class_attributes=attributes)
    send_method = attributes['send_message_extended']
    body = '{"message": "small text"}'

    # send
    res = send_method(QueueUrl=sqs_client_queue['QueueUrl'], MessageBody=body)
    assert 'MessageId' in res

    # receive
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'])
    assert 'Messages' in res
    assert len(res['Messages']) == 1
    assert (
        's3BucketName' in res['Messages'][0]['Body'] and
        's3Key' in res['Messages'][0]['Body'])
    assert json.loads(
        res['Messages'][0]['Body'])['s3BucketName'] == bucket_name

    # delete
    delete_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        ReceiptHandle=res['Messages'][0]['ReceiptHandle'])


# ##
# # large multiple send/receive/delete test
# ##

def test_multiple_extended_messaging_w_large_text(
        s3_bucket, sqs_client_queue, sqs_client, s3_client, big_message,
        bucket_name,
        send_message_batch_extended_client, receive_message_extended_client,
        delete_message_batch_extended_client):
    body1 = big_message
    body2 = json.loads(big_message)
    body2['id'] = 'TEST_BIG_MESSAGE2'
    body2 = json.dumps(body2)

    # send
    res = send_message_batch_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        Entries=[
            {'Id': '1', 'MessageBody': body1},
            {'Id': '2', 'MessageBody': body2}])

    assert 'Successful' in res
    assert len(res['Successful']) == 2
    assert list(map(lambda x: x['Id'], res['Successful'])) == ['1', '2']

    res = receive_message_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'], MessageAttributeNames=['All'],
        MaxNumberOfMessages=10)
    rhs = list(map(lambda x: x['ReceiptHandle'], res['Messages']))

    # delete
    delete_message_batch_extended_client(
        QueueUrl=sqs_client_queue['QueueUrl'],
        Entries=[
            {'Id': '1', 'ReceiptHandle': rhs[0]},
            {'Id': '2', 'ReceiptHandle': rhs[1]},
        ])
    res = sqs_client.receive_message(
        QueueUrl=sqs_client_queue['QueueUrl'], WaitTimeSeconds=1)
    assert 'Messages' not in res

    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['KeyCount'] == 0
