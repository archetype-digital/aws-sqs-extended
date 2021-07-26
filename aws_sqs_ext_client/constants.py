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
from enum import Enum


class SQSExtendedConstants(Enum):
    RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize"
    MESSAGE_POINTER_CLASS = (
        'software.amazon.payloadoffloading.PayloadS3Pointer')
    DEFAULT_MESSAGE_SIZE_THRESHOLD = 2**18
    S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
    S3_KEY_MARKER = "-..s3Key..-"
    RECEIPT_HANDLER_MATCHER = (
        r"^-\.\.s3BucketName\.\.-(.*)-\.\.s3BucketName\.\.-"
        r"-\.\.s3Key\.\.-(.*)-\.\.s3Key\.\.-(.*)$")
