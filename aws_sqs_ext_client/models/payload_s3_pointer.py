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
import typing

from ..constants import SQSExtendedConstants


class PayloadS3Pointer(object):
    """Model of the SQS message payload with S3 pointer toward stored message.
    this has compatibility with other libraries, like
    https://github.com/awslabs/payload-offloading-java-common-lib-for-aws/blob/master/src/main/java/software/amazon/payloadoffloading/PayloadS3Pointer.java.
    :type s3BucketName: str
    :param s3BucketName: s3 bucket name
    :type s3Key: str
    :param s3Key: s3 object key
    """

    def __init__(self, bucket_name: str, key: str) -> None:
        self.s3BucketName = bucket_name
        self.s3Key = key

    def toJSON(self) -> str:
        return json.dumps(
            self, default=lambda o: o.__dict__, sort_keys=True)

    @classmethod
    def fromJSON(cls, serialized: str) -> typing.Optional['PayloadS3Pointer']:
        try:
            data = json.loads(serialized)
            # support java client
            # https://docs.aws.amazon.com/sns/latest/dg/large-message-payloads.html
            if (isinstance(data, list) and len(data) == 2 and
                    data[0] == (
                        SQSExtendedConstants.MESSAGE_POINTER_CLASS.value)):
                data = data[1]
        except json.decoder.JSONDecodeError as e:
            raise ValueError(f'invalid json data: {e}')

        if not ('s3BucketName' in data and 's3Key' in data):
            raise ValueError(
                'invalid json data. s3BucketName and s3Key must be keys')

        return cls(data.get('s3BucketName'), data.get('s3Key'))
