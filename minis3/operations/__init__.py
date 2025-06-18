# -*- coding: utf-8 -*-
"""
minis3.operations.base
~~~~~~~~~~~~~~~~~~~~~~

Base classes for S3 request implementations.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import requests

from ..util import stringify


class S3Request(object):
    """
    Base class for all S3 requests.

    Handles common functionality like URL generation, authentication,
    and basic request setup that all S3 operations need.

    Args:
        conn: The S3 connection object
        params (dict, optional): Query parameters to add to the request URL
    """

    def __init__(self, conn, params=None):
        """
        Initialize the S3 request.

        Args:
            conn: The connection object with auth, endpoint, etc.            params (dict, optional): URL query parameters
        """
        self.auth = conn.auth
        self.tls = conn.tls
        self.endpoint = conn.endpoint
        self.path_style = getattr(
            conn, "path_style", False
        )  # Support for path-style URLs
        self.verify = conn.verify
        self.params = params or {}

    def bucket_url(self, key, bucket):
        """
        Generate the complete URL for an S3 request.

        Constructs URLs in the format: protocol://bucket.endpoint/key?params

        Args:
            key (str): The S3 object key (can be empty for bucket operations)
            bucket (str): The S3 bucket name

        Returns:
            str: Complete URL for the S3 request

        Examples:
            >>> request.bucket_url('my-file.txt', 'my-bucket')
            'https://my-bucket.s3.amazonaws.com/my-file.txt'
              >>> request.bucket_url('', 'my-bucket')  # Bucket operation
            'https://my-bucket.s3.amazonaws.com/'
        """
        protocol = "https" if self.tls else "http"
        key = stringify(key)
        bucket = stringify(bucket)

        # Ensure we have proper strings for URL operations
        if hasattr(key, "decode"):  # bytes object
            key = key.decode("utf-8")
        if hasattr(bucket, "decode"):  # bytes object
            bucket = bucket.decode("utf-8")

        # Convert to string if needed
        key = str(key) if key else ""
        bucket = str(bucket) if bucket else ""

        # Support both URL styles based on connection configuration
        if self.path_style:
            # Path-style: http://endpoint/bucket/key (for MinIO, etc.)
            url = "{0}://{1}/{2}/{3}".format(
                protocol, self.endpoint, bucket, key.lstrip("/")
            )
        else:
            # Virtual host-style: http://bucket.endpoint/key (default AWS style)
            url = "{0}://{1}.{2}/{3}".format(
                protocol, bucket, self.endpoint, key.lstrip("/")
            )

        # Add query parameters if present
        if self.params:
            url += self._build_query_string()

        return url

    def _build_query_string(self):
        """
        Build query string from parameters.

        Returns:
            str: Query string starting with '?' or empty string
        """
        if not self.params:
            return ""

        query_parts = []
        # Sort params for consistent URLs (important for testing)
        for param, value in sorted(self.params.items()):
            if value is not None:
                query_parts.append("{0}={1}".format(param, value))
            else:
                query_parts.append(param)

        return "?" + "&".join(query_parts) if query_parts else ""

    def adapter(self):
        """
        Get the HTTP adapter for making requests.

        Returns the requests module by default, but can be overridden
        for testing with mock adapters.

        Returns:
            module: The requests module or a mock adapter
        """
        return requests

    def run(self):
        """
        Execute the S3 request.

        This method must be implemented by subclasses to define
        the specific HTTP operation to perform.

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement the run() method")

    def _make_request(self, method, url, **kwargs):
        """
        Make an HTTP request with common S3 settings.

        Handles authentication, SSL verification, and error handling
        that's common to all S3 requests.

        Args:
            method (str): HTTP method ('GET', 'PUT', 'POST', 'DELETE', 'HEAD')
            url (str): Request URL
            **kwargs: Additional arguments to pass to requests

        Returns:
            Response: The HTTP response object

        Raises:
            HTTPError: If the request fails
        """
        # Ensure we always pass auth and verify
        kwargs.setdefault("auth", self.auth)
        kwargs.setdefault("verify", self.verify)

        # Make the request
        adapter = self.adapter()
        response = getattr(adapter, method.lower())(url, **kwargs)

        # Raise an exception for HTTP errors
        response.raise_for_status()

        return response
