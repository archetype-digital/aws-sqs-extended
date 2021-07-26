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

import pytest
from aws_sqs_ext_client.models.payload_s3_pointer import PayloadS3Pointer


class TestPayloadS3Pointer(object):
    '''tests for PayloadS3Pointer
    '''

    def test_toJSON(self):
        poi = PayloadS3Pointer('bucket', 'key')
        assert poi.s3BucketName == 'bucket'
        assert poi.s3Key == 'key'
        assert poi.toJSON() == '{"s3BucketName": "bucket", "s3Key": "key"}'

    def test_fromJSON(self):
        poi = PayloadS3Pointer.fromJSON(
            '{"s3BucketName": "bucket", "s3Key": "key"}')
        assert poi.s3BucketName == 'bucket'
        assert poi.s3Key == 'key'

    def test_fromJSON_w_pointer(self):
        poi = PayloadS3Pointer.fromJSON(
            '["software.amazon.payloadoffloading.PayloadS3Pointer",'
            '{"s3BucketName": "bucket", "s3Key": "key"}]')
        assert poi.s3BucketName == 'bucket'
        assert poi.s3Key == 'key'

    def test_fromJSON_invalid_str(self):
        with pytest.raises(ValueError) as excinfo:
            PayloadS3Pointer.fromJSON('abc')
        assert (
            'invalid json data: Expecting value: line 1 column 1 (char 0)'
            in str(excinfo.value))

        with pytest.raises(ValueError) as excinfo:
            PayloadS3Pointer.fromJSON('{}')
        assert (
            'invalid json data. s3BucketName and s3Key must be keys'
            in str(excinfo.value))

        with pytest.raises(ValueError) as excinfo:
            PayloadS3Pointer.fromJSON(
                '["software.amazon.payloadoffloading",'
                '{"s3BucketName": "bucket", "s3Key": "key"}]')
        assert (
            'invalid json data. s3BucketName and s3Key must be keys'
            in str(excinfo.value))

        with pytest.raises(ValueError) as excinfo:
            PayloadS3Pointer.fromJSON(
                '["software.amazon.payloadoffloading.PayloadS3Pointer",'
                '{"s3BucketName": "bucket", "s3Key": "key"},'
                '{"s3BucketName": "bucket", "s3Key": "key"}]')
        assert (
            'invalid json data. s3BucketName and s3Key must be keys'
            in str(excinfo.value))
