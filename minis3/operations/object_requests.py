# -*- coding: utf-8 -*-
"""
minis3.operations.object_requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

S3 object-level operations (upload, download, delete, copy, etc.)
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import mimetypes
import os

# Python 2/3 compatibility
try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

from ..util import LenWrapperStream, stringify
from . import S3Request


class GetRequest(S3Request):
    """
    Download an object from S3.

    Retrieves the content and metadata of an S3 object.

    Args:
        conn: S3 connection object
        key (str): S3 object key to download
        bucket (str): S3 bucket name
        headers (dict, optional): Additional HTTP headers
    """

    def __init__(self, conn, key, bucket, headers=None):
        super(GetRequest, self).__init__(conn)
        self.key = key
        self.bucket = bucket
        self.headers = headers or {}

    def run(self):
        """
        Execute the download request.

        Returns:
            Response: HTTP response containing the object data
        """
        url = self.bucket_url(self.key, self.bucket)
        return self._make_request("GET", url, headers=self.headers)


class UploadRequest(S3Request):
    """
    Upload an object to S3.

    Handles file uploads with optional metadata, cache control,
    content type detection, and access control.
    Args:
        conn: S3 connection object
        key (str): S3 object key for the upload
        local_file: File-like object to upload
        bucket (str): S3 bucket name
        expires: Cache expiration setting
        content_type (str, optional): MIME type of the file
        public (bool): Whether to make the object publicly readable (default: False for security)
        extra_headers (dict, optional): Additional HTTP headers
        close (bool): Whether to close the file after upload
        rewind (bool): Whether to seek to beginning before upload
    """

    def __init__(
        self,
        conn,
        key,
        local_file,
        bucket,
        expires=None,
        content_type=None,
        public=False,
        extra_headers=None,
        close=False,
        rewind=True,
    ):
        super(UploadRequest, self).__init__(conn)
        self.key = key
        self.fp = local_file
        self.bucket = bucket
        self.expires = expires
        self.content_type = content_type
        self.public = public
        self.extra_headers = extra_headers or {}
        self.close = close
        self.rewind = rewind

    def run(self):
        """
        Execute the upload request.

        Returns:
            Response: HTTP response from the upload operation
        """
        headers = self._build_headers()

        # Prepare the file for upload
        if self.rewind and hasattr(self.fp, "seek"):
            self.fp.seek(0, os.SEEK_SET)

        try:
            # Wrap file for proper length handling
            data = LenWrapperStream(self.fp)

            # Perform the upload
            url = self.bucket_url(self.key, self.bucket)
            return self._make_request("PUT", url, data=data, headers=headers)

        finally:
            # Clean up if requested
            if self.close and hasattr(self.fp, "close"):
                self.fp.close()

    def _build_headers(self):
        """
        Build HTTP headers for the upload request.

        Returns:
            dict: Complete set of headers for the upload
        """
        headers = {}

        # Cache control headers
        if self.expires is not None:
            headers["Cache-Control"] = self._calculate_cache_control()

        # Content type detection
        headers["Content-Type"] = self._determine_content_type()

        # Access control
        if self.public:
            headers["x-amz-acl"] = "public-read"

        # Merge with extra headers
        headers.update(self.extra_headers)

        return headers

    def _determine_content_type(self):
        """
        Determine the content type for the upload.

        Returns:
            str: MIME content type
        """
        if self.content_type:
            return self.content_type
        # Try to guess from filename
        guessed_type, _ = mimetypes.guess_type(self.key)
        return guessed_type or "application/octet-stream"

    def _calculate_cache_control(self):
        """
        Calculate Cache-Control header value.

        Returns:
            str: Cache-Control header value
        """
        expires = self.expires

        # Handle special 'max' value
        if expires == "max":
            expires = datetime.timedelta(seconds=31536000)  # 1 year
        elif isinstance(expires, int):
            expires = datetime.timedelta(seconds=expires)

        # Calculate max-age
        max_age_seconds = self._get_total_seconds(expires)
        cache_control = "max-age={0}".format(max_age_seconds)

        # Add visibility directive based on public/private setting
        if self.public:
            cache_control += ", public"
        else:
            cache_control += ", private"

        return cache_control

    def _get_total_seconds(self, timedelta_obj):
        """
        Get total seconds from timedelta (Python 2.6 compatibility).

        Args:
            timedelta_obj: datetime.timedelta object

        Returns:
            int: Total seconds
        """
        return timedelta_obj.days * 24 * 60 * 60 + timedelta_obj.seconds


class DeleteRequest(S3Request):
    """
    Delete an object from S3.

    Args:
        conn: S3 connection object
        key (str): S3 object key to delete
        bucket (str): S3 bucket name
    """

    def __init__(self, conn, key, bucket):
        super(DeleteRequest, self).__init__(conn)
        self.key = key
        self.bucket = bucket

    def run(self):
        """
        Execute the delete request.

        Returns:
            Response: HTTP response from the delete operation
        """
        url = self.bucket_url(self.key, self.bucket)
        return self._make_request("DELETE", url)


class CopyRequest(S3Request):
    """
    Copy an object within S3 or between buckets.

    Args:
        conn: S3 connection object
        from_key (str): Source object key
        from_bucket (str): Source bucket name
        to_key (str): Destination object key
        to_bucket (str): Destination bucket name
        metadata (dict, optional): New metadata for the copied object
        public (bool): Whether to make the copied object publicly readable
    """

    def __init__(
        self,
        conn,
        from_key,
        from_bucket,
        to_key,
        to_bucket,
        metadata=None,
        public=False,
    ):
        super(CopyRequest, self).__init__(conn)

        # Prepare source and destination paths
        self.from_key = quote(stringify(from_key.lstrip("/")))
        self.from_bucket = stringify(from_bucket)
        self.to_key = stringify(to_key.lstrip("/"))
        self.to_bucket = stringify(to_bucket)
        self.metadata = metadata
        self.public = public

    def run(self):
        """
        Execute the copy request.

        Returns:
            Response: HTTP response from the copy operation
        """
        headers = self._build_copy_headers()
        url = self.bucket_url(self.to_key, self.to_bucket)
        return self._make_request("PUT", url, headers=headers)

    def _build_copy_headers(self):
        """
        Build headers for the copy request.

        Returns:
            dict: Headers for the copy operation
        """
        headers = {
            "x-amz-copy-source": "/{0}/{1}".format(self.from_bucket, self.from_key)
        }

        # Metadata handling
        if self.metadata:
            headers["x-amz-metadata-directive"] = "REPLACE"
            headers.update(self.metadata)
        else:
            headers["x-amz-metadata-directive"] = "COPY"

        # Access control
        if self.public:
            headers["x-amz-acl"] = "public-read"

        return headers


class UpdateMetadataRequest(CopyRequest):
    """
    Update metadata of an existing S3 object.

    This is implemented as a special case of copy operation where
    the source and destination are the same object.

    Args:
        conn: S3 connection object
        key (str): S3 object key to update
        bucket (str): S3 bucket name
        metadata (dict, optional): New metadata for the object
        public (bool): Whether to make the object publicly readable
    """

    def __init__(self, conn, key, bucket, metadata=None, public=False):
        super(UpdateMetadataRequest, self).__init__(
            conn, key, bucket, key, bucket, metadata=metadata, public=public
        )


class HeadRequest(S3Request):
    """
    Get metadata for an S3 object or bucket without downloading content.

    Args:
        conn: S3 connection object
        bucket (str): S3 bucket name
        key (str, optional): S3 object key (empty for bucket metadata)
        headers (dict, optional): Additional HTTP headers
    """

    def __init__(self, conn, bucket, key="", headers=None):
        super(HeadRequest, self).__init__(conn)
        self.bucket = bucket
        self.key = key
        self.headers = headers or {}

    def run(self):
        """
        Execute the HEAD request.

        Returns:
            Response: HTTP response containing only headers/metadata
        """
        url = self.bucket_url(self.key, self.bucket)
        return self._make_request("HEAD", url, headers=self.headers)
