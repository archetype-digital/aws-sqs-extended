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
import os

import botocore
import pytest
from aws_sqs_ext_client.session import SQSExtendedSession


def test_auto_injected_session():
    import boto3
    from aws_sqs_ext_client import SQSExtendedSession

    assert isinstance(boto3.session.Session(), SQSExtendedSession)
    assert isinstance(boto3.DEFAULT_SESSION, SQSExtendedSession)
    assert hasattr(boto3.DEFAULT_SESSION, 'extend_sqs')


def test_extend_session_wo_s3params():
    """Need either location constrain in args or environment vars"""
    session = SQSExtendedSession()

    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        session.extend_sqs('test-sqs-message-bucket')
    assert "CreateBucket" in str(excinfo.value)


def test_extend_session_w_env(region, queue_name, s3_client):
    os.environ['AWS_DEFAULT_REGION'] = region
    bucket_name = 'test-sqs-message-bucket'

    session = SQSExtendedSession()
    session.extend_sqs(bucket_name)

    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['ResponseMetadata']['HTTPStatusCode'] == 200

    client = session.client('sqs')
    assert hasattr(client, 'send_message_extended')
    assert hasattr(client, 'receive_message_extended')
    assert hasattr(client, 'delete_message_extended')
    assert hasattr(client, 'send_message_batch_extended')
    assert hasattr(client, 'delete_message_batch_extended')

    resource = session.resource('sqs')
    queue = resource.Queue(queue_name)
    assert hasattr(queue, 'send_message_extended')
    assert hasattr(queue, 'receive_messages_extended')
    assert hasattr(queue, 'send_messages_extended')
    assert hasattr(queue, 'delete_messages_extended')

    message = resource.Message('queue_url', 'receipt_handle')
    assert hasattr(message, 'delete_extended')

    del os.environ['AWS_DEFAULT_REGION']


def test_extend_session_w_s3params(s3_client):
    bucket_name = 'test-sqs-message-bucket'
    session = SQSExtendedSession()
    session.extend_sqs(
        bucket_name,
        s3_bucket_params={
            'CreateBucketConfiguration': {
                'LocationConstraint': 'ap-northeast-1'
            }})

    res = s3_client.list_objects_v2(Bucket=bucket_name)
    assert res['ResponseMetadata']['HTTPStatusCode'] == 200


# @mock_s3
# def test_extend_session_w_already_exist_bucket(
#         aws_credentials, bucket_name, region):
#     """Test to check if the extend_sqs catches an exception
#     BucketAlreadyOwnedByYou and returns no error.
#     But, with any attemptions, cannot get BucketAlreadyOwnedByYou instead of
#     BucketAlreadyExists by using moto.
#     So, currently, comment this out.
#     But, we already checked in integration tests in which there are no issues
#     with multiple sessions.
#     """
#     os.environ['AWS_DEFAULT_REGION'] = region
#     SQSExtendedSession().extend_sqs(bucket_name)
#     SQSExtendedSession().extend_sqs(bucket_name)

#     del os.environ['AWS_DEFAULT_REGION']
