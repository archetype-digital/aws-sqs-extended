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
import copy
import hashlib
import logging
import re
import typing
import uuid

from .constants import SQSExtendedConstants
from .models.payload_s3_pointer import PayloadS3Pointer

logger = logging.getLogger(__name__)


class SQSExtendedMessage(object):
    """AWS SQS extended messaging class that gives some methods
    to send SQS larger messages than the limitation by using S3.
    :type session: object
    :param session: boto3 session that is used for sqs and s3 access
    :type s3_bucket_name: string
    :param s3_bucket_name: S3 bucket name to store actual messages
    :type always_through_s3: bool
    :param always_through_s3: if True, put all actual messages
        that are even smaller than threshold (optional: by default, it's True)
    :type message_size_threshold: int
    :param message_size_threshold: threshold to put actual message in S3
        (optional: default value is the SQS limitation 262,144)
    """

    def __init__(
            self, session, s3_bucket_name, always_through_s3=False,
            message_size_threshold=(
                SQSExtendedConstants.DEFAULT_MESSAGE_SIZE_THRESHOLD.value)):
        self.s3 = session.resource('s3')
        self.s3_bucket_name = s3_bucket_name
        self.message_size_threshold = message_size_threshold
        self.always_through_s3 = always_through_s3

    def _build_attributes_and_message(
        self, attributes: dict, body: str,
        s3_put_params: dict = {'ACL': 'private'},
    ) -> typing.Tuple[dict, str]:
        """Build attributes and message to be sent into the queue.
        This method does:
        - checks the amount of size of both attributes and body
        - if the amount of size is bigger than threshold, set the new of them
          and store actual body into S3

        :type attributes: dict
        :param attributes: message attributes
        :type body: str
        :param body: message body
        :type s3_put_params: dict
        :param s3_put_params: parameters for s3.put_object
        :rtype: tuple
        :return: tuple of re-built attributes and message body
        """
        encoded = body.encode()
        if not (self.always_through_s3 or
                self._is_message_larger(attributes, encoded)):
            return attributes, body

        # build the new attr
        reserved = {
            'DataType': 'Number', 'StringValue': str(len(encoded))}
        attributes[
            SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value
        ] = reserved

        # put actual message into S3
        s3_put_params['Key'] = str(uuid.uuid4())
        s3_put_params['Body'] = encoded
        s3_put_params['ContentLength'] = len(encoded)
        # if error happens, this raises exception,
        # like botocore.errorfactory.NoSuchBucket.
        # as well, that exception doesn't have to be catched
        # because it happens before sending a message into queue.
        self.s3.Bucket(self.s3_bucket_name).put_object(**s3_put_params)
        logger.info(
            f"{s3_put_params['Key']} was written into {self.s3_bucket_name}")

        # build the new message
        body = PayloadS3Pointer(
            self.s3_bucket_name, s3_put_params['Key']).toJSON()

        return attributes, body

    def _revert_attributes_and_message(
        self, attributes: typing.Optional[dict], body: str,
        receipt_handle: str,
    ) -> typing.Tuple[typing.Optional[dict], str, str]:
        """Revert attributes and message from the queue.
        :type attributes: dict
        :param attributes: message attributes
        :type body: str
        :param body: message body
        :type receipt_handle: str
        :param receipt_handle: identifier to handle received message
        :rtype: tuple
        :return: tuple of re-built attributes, message body, and receipt handle
        """
        if (attributes is None or attributes.get(
                SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value) is None):
            return attributes, body, receipt_handle

        payload = PayloadS3Pointer.fromJSON(body)

        # if error happens, this raises exception,
        # like botocore.errorfactory.NoSuchBucket.
        # as well, that exception doesn't have to be catched, and
        # stored message should be remained before deleting.
        data = self.s3.Bucket(payload.s3BucketName).Object(
            payload.s3Key).get()
        data = data['Body'].read().decode()
        logger.info(
            f"{payload.s3Key} was read from {payload.s3BucketName}")

        # pop special attribute for s3 association
        attr = copy.deepcopy(attributes)
        del attr[SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value]
        if not attr:
            attr = None

        # for deletion, edit receipt handle
        # this follows java extended client way
        # https://github.com/awslabs/amazon-sqs-java-extended-client-lib/blob/0208e1ad81351e5b90d5e3a413b5caea260ceb5f/src/main/java/com/amazon/sqs/javamessaging/AmazonSQSExtendedClient.java#L1138
        receipt_handle = (
            f'{SQSExtendedConstants.S3_BUCKET_NAME_MARKER.value}'
            f'{payload.s3BucketName}'
            f'{SQSExtendedConstants.S3_BUCKET_NAME_MARKER.value}'
            f'{SQSExtendedConstants.S3_KEY_MARKER.value}{payload.s3Key}'
            f'{SQSExtendedConstants.S3_KEY_MARKER.value}'
            f'{receipt_handle}'
        )

        return attr, data, receipt_handle

    def _delete_message_from_s3(self, receipt_handle: str) -> None:
        """Delete message stored in S3.
        :type receipt_handle: str
        :param receipt_handle: identifier to handle received message
        """
        bucket, key, _ = self._parse_receipt_handle(receipt_handle)
        # if error happens, this raises exception,
        # like botocore.errorfactory.NoSuchBucket.
        # as well, that exception doesn't have to be catched
        # because it happens before deleting a message into queue.
        self.s3.Bucket(bucket).Object(key).delete()
        logger.info(
            f"{key} was deleted from {bucket}")

    def _is_message_larger(self, attributes: dict, body: str):
        total = 0
        total += len(body)
        for key, value in attributes.items():
            total += len(key.encode())
            total += (
                len(str(value['DataType']).encode()) if 'DataType' in value
                else 0
            )
            total += (
                len(str(value['StringValue']).encode())
                if 'StringValue' in value else 0
            )
            if 'BinaryValue' in value:
                # if not hasattr(value['BinaryValue'], 'decode'),
                # send_message occures error anyway
                total += len(value['BinaryValue'])

        return total > self.message_size_threshold

    def _parse_received_response(
        self, sqs_response: typing.Any
    ) -> typing.Tuple[bool, list, typing.Optional[dict]]:
        """Parse response from receive_message.
        :type sqs_response: any (dict or list that depend on caller)
        :param sqs_response: response from receive_message(s)
        :rtype: (bool, list, dict)
        :return: two values of flag of client, message list, and metadata
        """
        is_client = False
        messages = sqs_response
        metadata = None
        if isinstance(sqs_response, dict):
            is_client = True
            messages = sqs_response.get('Messages', [])
            metadata = copy.deepcopy(sqs_response)
            if 'Messages' in sqs_response:
                del metadata['Messages']

        return is_client, messages, metadata

    def _parse_received_message(
        self, is_client: bool, message: typing.Any
    ) -> typing.Tuple[typing.Optional[dict], str, str]:
        """Parse each received message.
        :type is_client: bool
        :param is_client: True if the caller is client
        :type message: any (dict or sqs.Message that depend on caller)
        :param message: received message
        :rtype: (dict, str, str)
        :return: three values of attributes, body, and receipt handle
        """
        if is_client:
            attributes = message.get('MessageAttributes')
            body = message.get('Body')
            receipt_handle = message.get('ReceiptHandle')
        else:
            attributes = message.message_attributes
            body = message.body
            receipt_handle = message.receipt_handle

        return attributes, body, receipt_handle

    def _update_received_message(
        self, message: typing.Any, is_client: bool,
        attributes: typing.Optional[dict], body: str, receipt_handle: str,
    ) -> None:
        """Update received message.
        :type message: any (dict or sqs.Message that depend on caller)
        :param message: received original message to be updated
        :type is_client: bool
        :param is_client: True if the caller is client
        :type attributes: dict
        :param attributes: message attributes updated into message
        :type body: str
        :param body: message body updated into message
        :type receipt_handle: str
        :param receipt_handle: receipt handle updated into message
        :rtype: (dict, str, str)
        :return: three values of attributes, body, and receipt handle
        """
        # calculate md5 digest for both body and attributes
        md5_of_body = hashlib.md5(body.encode()).hexdigest()
        md5_of_message_attributes = self._md5attributes(attributes)

        # update message with modified body and attributes
        if is_client:
            if attributes:
                message['MessageAttributes'] = attributes
                message['MD5OfMessageAttributes'] = md5_of_message_attributes
            elif 'MessageAttributes' in message:
                # in this case, message.attributes includes only our attribute
                del message['MessageAttributes']
                del message['MD5OfMessageAttributes']

            message['Body'] = body
            message['ReceiptHandle'] = receipt_handle
            message['MD5OfBody'] = md5_of_body
        else:
            if attributes:
                message.meta.data['MessageAttributes'] = attributes
                message.meta.data['MD5OfMessageAttributes'] = (
                    md5_of_message_attributes)
            elif 'MessageAttributes' in message.meta.data:
                # in this case, message.attributes includes only our attribute
                del message.meta.data['MessageAttributes']
                del message.meta.data['MD5OfMessageAttributes']

            message.meta.data['Body'] = body
            # NOTE:
            # attributes of class identifiers are read-only.
            # https://github.com/boto/boto3/blob/master/boto3/resources/factory.py#L284
            # so that we have to take two receipt handles carefully on methods
            # to use receipt handle, like delete_message
            message.meta.data['ReceiptHandle'] = receipt_handle
            message.meta.data['MD5OfBody'] = md5_of_body

    def _is_extended_receipt_handle(self, receipt_handle: str) -> bool:
        """Check if the given receipt handle associates with extended message.
        :type receipt_handle: str
        :param receipt_handle: receipt handle associated with received message
        :rtype: bool
        :return: True if the given one is associated with extended message
        """
        prog = re.compile(SQSExtendedConstants.RECEIPT_HANDLER_MATCHER.value)
        return prog.match(receipt_handle) is not None

    def _get_original_receipt_handle(
            self, receipt_handle: str) -> typing.Optional[str]:
        """Return the original one searched from given handle.
        :type receipt_handle: str
        :param receipt_handle: receipt handle associated with received message
        :rtype: str
        :return: original receipt handle
        """
        prog = re.compile(SQSExtendedConstants.RECEIPT_HANDLER_MATCHER.value)
        match = prog.match(receipt_handle)
        return match.group(3) if match is not None else None

    def _parse_receipt_handle(
        self, receipt_handle: str
    ) -> typing.Optional[typing.Tuple[str, str, str]]:
        """Return bucket name, key name, and original receopt handle
        implemented in the given receipt handle.
        :type receipt_handle: str
        :param receipt_handle: receipt handle associated with received message
        :rtype: (str, str, str)
        :return: original receipt handle
        """
        prog = re.compile(SQSExtendedConstants.RECEIPT_HANDLER_MATCHER.value)
        match = prog.match(receipt_handle)
        return (
            (match.group(1), match.group(2), match.group(3))
            if match is not None else None)

    def _md5attributes(self, attributes: dict) -> str:
        """Calcuate md5 digest of message attributes.
        Note that AWS SQS calculates it with big endian.
        :type attributes: dict
        :param attributes: sqs message attributes
        :rtype: str
        :return: md5 of the given message attributes
        """
        if not attributes or not isinstance(attributes, dict):
            return None

        seq = b''
        items = sorted(attributes.items())
        for k, v in items:
            val = (
                v['StringValue'].encode() if 'StringValue' in v else
                v['BinaryValue'] if 'BinaryValue' in v else None)
            if val is None:
                # this case never happens when called by receive method
                # because send method refuses to send the invalid attrs
                logger.warn(f'thera are no supported attribute value in {k}')
                continue

            seq += len(k.encode()).to_bytes(4, 'big')
            seq += k.encode()
            seq += len(v['DataType'].encode()).to_bytes(4, 'big')
            seq += v['DataType'].encode()
            seq += (
                bytes([1])
                if v['DataType'] == 'String' or v['DataType'] == 'Number' else
                bytes([2]))
            seq += len(val).to_bytes(4, 'big')
            seq += val

        return hashlib.md5(seq).hexdigest()

    def _send_message_extended(self, func: typing.Callable) -> typing.Callable:
        """This method returns inner actual 'extended send method'
        to the client/resource event handler.
        """

        def send_message_extended(*args, **kwargs) -> typing.Any:
            """Send a message (and attributes) to the given queue.
            If the amount size of message and attributes are larger than
            the threshold, this method puts original message onto S3 bucket
            and sends metadata as a message into the queue.
            If queue is the standard, the following arguments are required.
            :type QueueUrl: str
            :param QueueUrl: the url of queue (only for client, not resource)
            :type MessageBody: str
            :param MessageBody: message body
            :rtype: any
            :return: depends on the original function.

            If queue is the FIFO, additional arguments are required.
            :type MessageDeduplicationId: str
            :param MessageDeduplicationId: message duplication ID
            :type MessageGroupId: str
            :param MessageGroupId: message group ID

            Sending message has some restrictions that are checked
            by original method send_message.
            For example, number of keys of MessageAttributes
            (should be less than 10), required parameters
            (both MessageDeduplicationId and MessageGroupId are needed to FIFO)
            are checked by send_message.
            """
            attributes = kwargs.get('MessageAttributes', {})
            if attributes.get(
                    SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value):
                raise ValueError(
                    f'{SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value}'
                    ' is reserved name')

            body = kwargs.get('MessageBody', None)
            if body is None:
                raise ValueError('message body is required')

            kwargs['MessageAttributes'], kwargs['MessageBody'] = (
                self._build_attributes_and_message(attributes, body))

            return func(*args, **kwargs)

        return send_message_extended

    def _receive_message_extended(
            self, func: typing.Callable) -> typing.Callable:
        """This method returns inner actual 'extended receive method'
        to the client/resource event handler.
        """

        def receive_message_extended(*args, **kwargs) -> typing.Any:
            """Receive messages (and attributes) from the given queue.
            If the attribute includes RESERVED_ATTRIBUTE_NAME, this method will
            get actual messages from S3 bucket.
            If queue is the standard, the following arguments are required.
            :type QueueUrl: str
            :param QueueUrl: the url of queue (only for client, not resource)
            :rtype: any
            :return: depends on the original function.

            Receiving message has some restrictions that are checked
            by original method send_message.
            For example, number of keys of MessageAttributes
            (should be less than 10) is checked by receive_message(s).
            """
            # check attributes names that should be returned from queue
            # and add necessary one
            kwargs['AttributeNames'] = kwargs.get('AttributeNames', ['All'])
            kwargs['MessageAttributeNames'] = kwargs.get(
                'MessageAttributeNames', [])
            if (not isinstance(kwargs['AttributeNames'], list) or
                    not isinstance(kwargs['MessageAttributeNames'], list)):
                raise ValueError(
                    'AttributeNames or MessageAttributeNames must be list')

            if not (
                'All' in kwargs['MessageAttributeNames'] or
                '.*' in kwargs['MessageAttributeNames']
            ) and (
                SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value not in
                kwargs['MessageAttributeNames']
            ):
                kwargs['MessageAttributeNames'].append(
                    SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value)

            # get message from queue
            response = func(*args, **kwargs)
            is_client, messages, metadata = self._parse_received_response(
                response)

            # transform messages
            for message in messages:
                attributes, body, receipt_handle = (
                    self._parse_received_message(is_client, message))

                attributes, body, receipt_handle = (
                    self._revert_attributes_and_message(
                        attributes, body, receipt_handle))

                self._update_received_message(
                    message, is_client, attributes, body, receipt_handle)

            # format response
            if is_client:
                if messages:
                    metadata['Messages'] = messages
                messages = metadata

            return messages

        return receive_message_extended

    def _delete_message_extended(
            self, func: typing.Callable) -> typing.Callable:
        """This method returns inner actual 'extended delete method'
        to the client/resource event handler.
        """

        def delete_message_extended(*args, **kwargs) -> None:
            """Delete messages (and attributes) from the given queue.
            If the attribute includes RESERVED_ATTRIBUTE_NAME, this method will
            delete actual messages from S3 bucket.
            When using client ver method, the following arguments are required.
            :type QueueUrl: str
            :param QueueUrl: the url of queue (only for client, not resource)
            :type ReceiptHandle: str
            :param ReceiptHandle: handler associated with received message

            Deleting message has some restrictions that are checked
            by original method delete_message.
            For example, number of keys of MessageAttributes
            (should be less than 10) is checked by receive_message(s).
            """
            # arg[0] must be self of sqs.Message when called by resource
            is_client = False
            receipt_handle = None
            if kwargs is not None and 'ReceiptHandle' in kwargs:
                is_client = True
                receipt_handle = kwargs.get('ReceiptHandle')
            elif len(args):
                # our lib might update receipt handle in meta
                # so that it's prioritized more than class attribute
                receipt_handle = args[0].meta.data.get(
                    'ReceiptHandle', args[0].receipt_handle)

            if receipt_handle is None:
                raise ValueError('invalid call without ReceiptHandle')

            if self._is_extended_receipt_handle(receipt_handle):
                self._delete_message_from_s3(receipt_handle)

                original = self._get_original_receipt_handle(receipt_handle)
                if is_client:
                    kwargs['ReceiptHandle'] = original
                else:
                    args[0].meta.data['ReceiptHandle'] = original

            func(*args, **kwargs)

        return delete_message_extended

    def _send_message_batch_extended(
            self, func: typing.Callable) -> typing.Callable:
        """This method returns inner actual 'extended batch send method'
        to the client/resource event handler.
        """

        def send_message_batch_extended(*args, **kwargs) -> dict:
            """Send messages (and attributes) to the given queue.
            If the amount size of message and attributes of each entry
            are larger than the threshold, this method puts original message
            onto S3 bucket and sends metadata as a message into the queue.
            When using client(), the following is required.
            :type QueueUrl: str
            :param QueueUrl: the url of queue (only for client, not resource)

            If queue is the standard, the following attributes of each entry
            are required.
            :type Entries: typing.List[str]
            :param Entries: list of message metadata

            If queue is the FIFO, additional arguments of each entry are
            required.
            :type MessageDeduplicationId: str
            :param MessageDeduplicationId: message duplication ID
            :type MessageGroupId: str
            :param MessageGroupId: message group ID

            :rtype: dict
            :return: result to write messages

            Sending message has some restrictions that are checked
            by original method send_message.
            For example, number of keys of MessageAttributes
            (should be less than 10), required parameters
            (both MessageDeduplicationId and MessageGroupId are needed to FIFO)
            are checked by send_message.
            """
            entries = kwargs.get('Entries')
            if not isinstance(entries, list):
                raise ValueError('Entries (list) must be given')

            for i, entry in enumerate(entries):
                attributes = entry.get('MessageAttributes', {})
                if attributes.get(
                        SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value):
                    raise ValueError(
                        f'{SQSExtendedConstants.RESERVED_ATTRIBUTE_NAME.value}'
                        f' is reserved name, found in {i}')

                body = entry.get('MessageBody')
                if body is None:
                    raise ValueError(f'message body is required, found in {i}')

                entry['MessageAttributes'], entry['MessageBody'] = (
                    self._build_attributes_and_message(attributes, body))

            return func(*args, **kwargs)

        return send_message_batch_extended

    def _delete_message_batch_extended(
            self, func: typing.Callable) -> typing.Callable:
        """This method returns inner actual 'extended batch delete method'
        to the client/resource event handler.
        """

        def delete_message_batch_extended(*args, **kwargs) -> dict:
            """Delete messages (and attributes) to the given queue.
            If the attribute includes RESERVED_ATTRIBUTE_NAME, this method will
            delete actual messages from S3 bucket.
            When using client ver method, the following arguments are required.
            :type QueueUrl: str
            :param QueueUrl: the url of queue (only for client, not resource)

            Both client and resource ver require the following.
            :type Entries: typing.List[str]
            :param Entries: list of message metadata

            :rtype: dict
            :return: result to write messages
            """
            entries = kwargs.get('Entries')
            if not isinstance(entries, list):
                raise ValueError('Entries (list) must be given')

            for i, entry in enumerate(entries):
                receipt_handle = entry.get('ReceiptHandle')
                if receipt_handle is None:
                    raise ValueError(f'missing ReceiptHandle, found {i}')

                if self._is_extended_receipt_handle(receipt_handle):
                    self._delete_message_from_s3(receipt_handle)

                    original = self._get_original_receipt_handle(
                        receipt_handle)
                    entry['ReceiptHandle'] = original

            return func(*args, **kwargs)

        return delete_message_batch_extended

    def add_send_message_extended(self, *args) -> typing.Callable:
        def add_custom_method(class_attributes: dict, **kwargs) -> None:
            class_attributes['send_message_extended'] = (
                self._send_message_extended(class_attributes['send_message']))

        return add_custom_method

    def add_receive_message_extended(self, event: str) -> typing.Callable:
        def add_custom_method(class_attributes: dict, **kwargs) -> None:
            if event == 'creating-client-class.sqs':
                class_attributes['receive_message_extended'] = (
                    self._receive_message_extended(
                        class_attributes['receive_message']))
            elif event == 'creating-resource-class.sqs.Queue':
                class_attributes['receive_messages_extended'] = (
                    self._receive_message_extended(
                        class_attributes['receive_messages']))

        return add_custom_method

    def add_delete_message_extended(self, event: str) -> typing.Callable:
        def add_custom_method(class_attributes: dict, **kwargs) -> None:
            if event == 'creating-client-class.sqs':
                class_attributes['delete_message_extended'] = (
                    self._delete_message_extended(
                        class_attributes['delete_message']))
            elif event == 'creating-resource-class.sqs.Message':
                class_attributes['delete_extended'] = (
                    self._delete_message_extended(
                        class_attributes['delete']))

        return add_custom_method

    def add_send_message_batch_extended(self, event: str) -> typing.Callable:
        def add_custom_method(class_attributes: dict, **kwargs) -> None:
            if event == 'creating-client-class.sqs':
                class_attributes['send_message_batch_extended'] = (
                    self._send_message_batch_extended(
                        class_attributes['send_message_batch']))
            elif event == 'creating-resource-class.sqs.Queue':
                class_attributes['send_messages_extended'] = (
                    self._send_message_batch_extended(
                        class_attributes['send_messages']))

        return add_custom_method

    def add_delete_message_batch_extended(self, event: str) -> typing.Callable:
        def add_custom_method(class_attributes: dict, **kwargs) -> None:
            if event == 'creating-client-class.sqs':
                class_attributes['delete_message_batch_extended'] = (
                    self._delete_message_batch_extended(
                        class_attributes['delete_message_batch']))
            elif event == 'creating-resource-class.sqs.Queue':
                class_attributes['delete_messages_extended'] = (
                    self._delete_message_batch_extended(
                        class_attributes['delete_messages']))

        return add_custom_method
