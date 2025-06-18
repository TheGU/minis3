# -*- coding: utf-8 -*-
"""
Bucket management operations for minis3.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from . import S3Request


class CreateBucketRequest(S3Request):
    """
    S3 request to create a bucket.

    This request creates a new bucket in the S3-compatible service.
    """

    def __init__(self, conn, bucket_name):
        """
        Initialize bucket creation request.

        Args:
            conn: S3 Connection object
            bucket_name (str): Name of the bucket to create
        """
        super(CreateBucketRequest, self).__init__(conn)
        self.bucket_name = bucket_name

    def run(self):
        """Execute the bucket creation request."""
        # Create URL for bucket creation (path-style)
        url = self.bucket_url("", self.bucket_name)

        # Make the PUT request to create the bucket
        response = self._make_request("PUT", url, data=b"")
        return response


class DeleteBucketRequest(S3Request):
    """
    S3 request to delete a bucket.

    This request deletes an empty bucket from the S3-compatible service.
    """

    def __init__(self, conn, bucket_name):
        """
        Initialize bucket deletion request.

        Args:
            conn: S3 Connection object
            bucket_name (str): Name of the bucket to delete
        """
        super(DeleteBucketRequest, self).__init__(conn)
        self.bucket_name = bucket_name

    def run(self):
        """Execute the bucket deletion request."""
        # Create URL for bucket deletion (path-style)
        url = self.bucket_url("", self.bucket_name)

        # Make the DELETE request to delete the bucket
        response = self._make_request("DELETE", url)
        return response
