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


import logging
import os
from typing import Optional

import boto3

from .constants import SQSExtendedConstants
from .extended_messaging import SQSExtendedMessage

logger = logging.getLogger(__name__)


class SQSExtendedSession(boto3.session.Session):
    """AWS SQS client for SQS extention.
    This class is inherited from boto3.session.Session,
    which just has an additional method to extend SQS messaging.
    So, arguments for the constractor are the same as the original.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def extend_sqs(
        self, s3_bucket_name: str, always_through_s3: bool = False,
        message_size_threshold: int = (
            SQSExtendedConstants.DEFAULT_MESSAGE_SIZE_THRESHOLD.value),
        s3_bucket_params: Optional[dict] = {'ACL': 'private'},
    ) -> None:
        """Initialize the SQS extended messaging.
        This method craetes S3 bucket if not exists, initializes a class for
        SQS extention.
        :type s3_bucket_name: string
        :param s3_bucket_name: S3 bucket name to store actual messages
        :type always_through_s3: bool
        :param always_through_s3: if True, put all actual messages
            that are even smaller than threshold
            (optional: True is given by default)
        :type message_size_threshold: int
        :param message_size_threshold: threshold to put actual message in S3
            (optional: default value is the SQS limitation 262,144)
        :param s3_bucket_params: parameter for S3 bucket creation
            used to store huge messages, like `{'ACL': 'private'}`.
            If None is set on this param, this module won't create S3 bucket.
            It's recommended to create a bucket for object storing
            preliminarily, witout creation by this module because
            you should create a bucket with some options, like the specific
            finite object lifecycle configured by
            `put_bucket_lifecycle_configuration`.
        """
        # create S3 bucket if needed
        if s3_bucket_params is not None:
            region = os.getenv('AWS_DEFAULT_REGION')
            if region and 'CreateBucketConfiguration' not in s3_bucket_params:
                s3_bucket_params['CreateBucketConfiguration'] = {
                    'LocationConstraint': region
                }

            s3_bucket_params['Bucket'] = s3_bucket_name
            s3 = self.client('s3')
            try:
                s3.create_bucket(**s3_bucket_params)
                logger.info(f'bucket {s3_bucket_name} was created')
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

        # initialize sqs extention
        sqs = SQSExtendedMessage(
            self, s3_bucket_name, always_through_s3, message_size_threshold)
        self.events.register(
            'creating-client-class.sqs',
            sqs.add_send_message_extended('creating-client-class.sqs')
        )
        self.events.register(
            'creating-client-class.sqs',
            sqs.add_receive_message_extended('creating-client-class.sqs')
        )
        self.events.register(
            'creating-client-class.sqs',
            sqs.add_delete_message_extended('creating-client-class.sqs')
        )
        self.events.register(
            'creating-client-class.sqs',
            sqs.add_send_message_batch_extended('creating-client-class.sqs')
        )
        self.events.register(
            'creating-client-class.sqs',
            sqs.add_delete_message_batch_extended('creating-client-class.sqs')
        )

        self.events.register(
            'creating-resource-class.sqs.Queue',
            sqs.add_send_message_extended('creating-resource-class.sqs.Queue')
        )
        self.events.register(
            'creating-resource-class.sqs.Queue',
            sqs.add_receive_message_extended(
                'creating-resource-class.sqs.Queue')
        )
        self.events.register(
            'creating-resource-class.sqs.Message',
            sqs.add_delete_message_extended(
                'creating-resource-class.sqs.Message')
        )
        self.events.register(
            'creating-resource-class.sqs.Queue',
            sqs.add_send_message_batch_extended(
                'creating-resource-class.sqs.Queue')
        )
        self.events.register(
            'creating-resource-class.sqs.Queue',
            sqs.add_delete_message_batch_extended(
                'creating-resource-class.sqs.Queue')
        )
