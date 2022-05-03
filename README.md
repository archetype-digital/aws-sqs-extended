# aws-sqs-ext-client

The Amazon SQS Extended Client Library for Python for sending and receiving large messages via S3. This aims to have the same capability of [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib), in which the client can send and receive messages larger than the SQS limit (256 KB), up to the limit of S3 (5 TB), in the similar way to [Boto3 - The AWS SDK for Python](https://github.com/boto/boto3). This library supports:

- Send/receive large messages over than threshold (by default, it's 2**18)
- Enable to send/receive all messages, even though the data size is under the threshold, by turning on `always_through_s3`
- Enable to configure the threshold to which size you want
- Enable to check message's MD5 chechsum when receiving the large message
- Enalbe to configure the S3 bucket, like its ACL, where the large messages are temporarily stored

## Prerequisites

This package requires AWS account and Python 3.7+ environment. Please configure an AWS account as well as prepare the Python by referring README of [boto3](https://github.com/boto/boto3). Or, just an example, `aws-vault` is the useful tool to handle AWS account, like `aws-vault exec PROFILE_USER -- python APP_WITH_THIS_LIB`.

## Installation

```sh
pip install aws-sqs-ext-client
```

## Usage

This section shows some of examples to use this library. Please see `test/integration/test_all.py` to know more.

### Extended methods

The table below shows extended methods to send/receive/delete large messages. Those APIs have same specifications as methods without "_extended" described in [SQS - boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html). For instance, `send_message_extended` of the client API accepts the same arguments as `send_message` of the client API.

| Types              | Methods                       | Description                                                |
|--------------------|-------------------------------|------------------------------------------------------------|
| Client             | send_message_extended         | send one large message                                     |
| Client             | receive_message_extended      | receive multiple large messages (with MaxNumberOfMessages) |
| Client             | delete_message_extended       | delete one large message                                   |
| Client             | send_message_batch_extended   | send multiple large messages                               |
| Client             | delete_message_batch_extended | delete multiple large messages                             |
| Resource (Queue)   | send_message_extended         | send one large message                                     |
| Resource (Queue)   | receive_messages_extended     | receive multiple large messages (with MaxNumberOfMessages) |
| Resource (Message) | delete_extended               | delete one large message                                   |
| Resource (Queue)   | send_messages_extended        | send multiple large messages                               |
| Resource (Queue)   | delete_messages_extended      | delete multiple large messages                             |

### Session Initialization

First of all, you need to initialize and extend the boto3 session.

```python
import boto3
# override boto3.session.Session and overwrite the default session
import aws_sqs_ext_client  # noqa: F401

# create session
# instead, you can use boto3.DEFAULT_SESSION
session = boto3.session.Session()

# extend the session
# can add the following options
# always_through_s3: bool: enable to store even small message into S3 (by default, it's False)
# message_size_threshold: int: like 2*10. enable to change the threshold (default value is 2**18)
# s3_bucket_params: dict: add parameters to create/check the bucket where this lib stores the messages.
#   By default, this parameter is `{'ACL': 'private'}`.
#   If you already created S3 bucket for storing huge messages and utilize it, set `s3_bucket_params=None`.
#   With non-None parameter, if you don't specify AWS_DEFAULT_REGION on the environment variables,
#   you need to specify the location constrain by
#   {'CreateBucketConfiguration': {'LocationConstraint': YOUR_REGION}}.
#   Available other parameters are shown in
#   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
# 
# It's recommended to create a bucket for object storing preliminarily even though this module gives you automatic creation functionality.
# That's because you should create a bucket with some options, like the specific finite object lifecycle configured by `put_bucket_lifecycle_configuration`.
session.extend_sqs('S3_BUCKET_NAME_TO_STORE_MESSAGES')
```

### with Resource

```python
# please initialize session like above

message = 'large string message more than threshold'

# create/get queue
# you can create both standard and fifo queue
sqs = session.resource('sqs')
queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})
# or using existing queue
queue = sqs.get_queue_by_name(QueueName='test')

# send message
# you can add any other arguments that are accepted in `send_message`,
# like `MessageAttributes` and `MessageDeduplicationId`
res = queue.send_message_extended(MessageBody=message)

# receive message
# you can add any other arguments that are accepted in `receive_messages`,
# like `VisibilityTimeout`
received = queue.receive_messages_extended(
    MessageAttributeNames=['All'], MaxNumberOfMessages=10,
    WaitTimeSeconds=5)
  
for r in received:
    # if you want, you can check received message with given MD5
    # the function `checkdata` should be given by you
    checkdata(r.body, r.meta.data['MD5OfBody'])
    checkdata(
      r.meta.data['MessageAttributes'], r.meta.data['MD5OfMessageAttributes'])

    # process whatever you want with a message

    # delete both a message from the queue and a data on S3 bucket
    # this should be called til visibility timeout is elapsed
    # see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
    r.delete_extended()
```

### with Client

```python
# please initialize session like above

message = 'large string message more than threshold'

# create/get queue
# you can create both standard and fifo queue
sqs = session.client('sqs')
queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})
# or using existing queue
queue = sqs.get_queue_by_name(QueueName='test')

# send message
# you can add any other arguments that are accepted in `send_message`,
# like `MessageAttributes` and `MessageDeduplicationId`
res = sqs.send_message_extended(
    QueueUrl=queue['QueueUrl'], MessageBody=message)

# receive message
# you can add any other arguments that are accepted in `receive_messages`,
# like `VisibilityTimeout`
received = sqs.receive_message_extended(
    QueueUrl=queue['QueueUrl'],  MessageAttributeNames=['All'],
    MaxNumberOfMessages=10, WaitTimeSeconds=5)
  
received = received['Messages'] if 'Messages' in received else []
for r in received:
    # if you want, you can check received message with given MD5
    # the function `checkdata` should be given by you
    checkdata(r['Body'], r['MD5OfBody'])
    checkdata(r['MessageAttributes'], r['MD5OfMessageAttributes'])

    # process whatever you want with a message

    # delete both a message from the queue and a data on S3 bucket
    # this should be called til visibility timeout is elapsed
    # see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
    sqs.delete_message_extended(
        QueueUrl=queue['QueueUrl'], ReceiptHandle=r['ReceiptHandle'])
```

### with Resource (multiple sending/deleting)

With multiple deletion method `delete_messages_extended`, please use receipt handles gotten from `sqs.Message.meta.data['ReceiptHandle']` instead of `sqs.Message.receipt_handle`. Because `sqs.Message.receipt_handle` is read-only attributes, the method `delete_messages_extended` cannot overwrite the "correct" handle. With the right handle from `sqs.Message.meta.data['ReceiptHandle']`, the method `delete_messages_extended` deletes both messages in the queue and data objects in the S3 bucket. Otherwise, it only deletes messages in the queue.

```python
# please initialize session like above

messages = [{
    'Id': '1', 'MessageBody': "large string message more than threshold",
}, {
    'Id': '2', 'MessageBody': "large string message more than threshold",
}]

# create/get queue
# you can create both standard and fifo queue
sqs = session.resource('sqs')
queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})
# or using existing queue
queue = sqs.get_queue_by_name(QueueName='test')

# send messages
# you can add any other arguments that are accepted in `send_messages`,
# like `MessageAttributes` and `MessageDeduplicationId`
res = queue.send_messages_extended(Entries=messages)

# receive messages
# you can add any other arguments that are accepted in `receive_messages`,
# like `VisibilityTimeout`
received = queue.receive_messages_extended(
    MessageAttributeNames=['All'], MaxNumberOfMessages=10,
    WaitTimeSeconds=5)

receipt_handles = []
for r in received:
    # if you want, you can check received message with given MD5
    # the function `checkdata` should be given by you
    checkdata(r.body, r.meta.data['MD5OfBody'])
    checkdata(
      r.meta.data['MessageAttributes'], r.meta.data['MD5OfMessageAttributes'])

    # process whatever you want with a message

    # aggreage receipt handle: use meta one instad of its attribute
    receipt_handles.append({
        'Id': str(i), 'ReceiptHandle': r.meta.data['ReceiptHandle']})

# delete both messages from the queue and data on S3 bucket
# this should be called til visibility timeout is elapsed
# see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
res = queue.delete_messages_extended(Entries=receipt_handles)
```

## Test

`tests/integration/test_all.py` gives you clues about how to use this module with AWS resources.

```sh
# AWS credentials, like AWS_ACCESS_KEY_ID, should be set preliminarily
python tests/integration/test_all.py
```

`tests/units` includes unit tests.

```sh
export TEST_THRESHOLD=90
python setup.py test
coverage report --fail-under=${TEST_THRESHOLD}
```

## Lint

```sh
flake8 aws_sqs_ext_client --count --show-source --statistics
```

## License

MIT License.