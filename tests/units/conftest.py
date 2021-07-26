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
import os
import uuid

import boto3
import pytest
from aws_sqs_ext_client.extended_messaging import SQSExtendedMessage
from moto import mock_s3, mock_sqs


@pytest.fixture(scope='session')
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'fake'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'fake'
    os.environ['AWS_SECURITY_TOKEN'] = 'fake'
    os.environ['AWS_SESSION_TOKEN'] = 'fake'


@pytest.fixture(scope='session')
def region():
    return 'ap-northeast-1'


@pytest.fixture
def session(aws_credentials):
    with mock_s3(), mock_sqs():
        session = boto3.session.Session()
        yield session


@pytest.fixture
def s3_client(aws_credentials, session, region):
    with mock_s3():
        s3_client = session.client('s3', region_name=region)
        yield s3_client


@pytest.fixture
def s3_resource(aws_credentials, session, region):
    with mock_s3():
        s3_resource = session.resource('s3', region_name=region)
        yield s3_resource


@pytest.fixture
def sqs_client(aws_credentials, session, region):
    with mock_sqs():
        sqs_client = session.client('sqs', region_name=region)
        yield sqs_client


@pytest.fixture
def sqs_resource(aws_credentials, session, region):
    with mock_sqs():
        sqs_resource = session.resource('sqs', region_name=region)
        yield sqs_resource


@pytest.fixture(scope='session')
def bucket_name():
    return 'extended-message-test-bucket'


@pytest.fixture(scope='session')
def queue_name():
    return 'extended-message-test-queue'


@pytest.fixture(scope='session')
def big_message():
    message = {'id': 'TEST_BIG_MESSAGE'}
    # len('uuid4') == 38 so that 2**18/(len(uuid4)*2) ~= 3450
    for _ in range(0, 3500):
        message[str(uuid.uuid4())] = str(uuid.uuid4())
    return json.dumps(message)


@pytest.fixture
def s3_bucket(s3_client, bucket_name, region):
    s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={
            'LocationConstraint': region})
    yield


@pytest.fixture
def sqs_client_queue(sqs_client, queue_name):
    queue = sqs_client.create_queue(QueueName=queue_name)
    yield queue


@pytest.fixture
def sqs_resource_queue(sqs_resource, queue_name):
    queue = sqs_resource.create_queue(QueueName=queue_name)
    yield queue


@pytest.fixture
def sqs_extended_message(session, bucket_name):
    return SQSExtendedMessage(session, bucket_name)
