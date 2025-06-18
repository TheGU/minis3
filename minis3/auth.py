# -*- coding: utf-8 -*-
"""
minis3.auth
~~~~~~~~~~~

Authentication module for S3-compatible services.
Provides AWS Signature Version 2 and Version 4 support.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from requests.auth import AuthBase

from .datetime_utils import get_utc_datetime
from .signatures import SignatureV2, SignatureV4


class S3Auth(AuthBase):
    """
    S3 Custom Authenticator class for requests.

    This authenticator will sign your requests using either AWS Signature Version 2
    or Version 4, making it compatible with AWS S3 and S3-compatible services.

    Supports:
        - AWS Signature Version 4 (default, recommended)
        - AWS Signature Version 2 (legacy compatibility)
        - Custom S3-compatible endpoints (MinIO, DigitalOcean Spaces, etc.)

    Args:
        access_key (str): Your S3 access key
        secret_key (str): Your S3 secret key
        signature_version (str): 's3v4' for Signature Version 4 (default) or 's3' for Version 2
        endpoint (str): S3 endpoint hostname (default: 's3.amazonaws.com')

    Examples:
        Basic AWS S3 usage:
        >>> auth = S3Auth('AKIAIOSFODNN7EXAMPLE', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
        >>> response = requests.get('https://bucket.s3.amazonaws.com/key', auth=auth)

        MinIO usage:
        >>> auth = S3Auth('minioadmin', 'minioadmin', endpoint='localhost:9000')
        >>> response = requests.get('http://bucket.localhost:9000/key', auth=auth)

        Legacy Signature Version 2:
        >>> auth = S3Auth('access', 'secret', signature_version='s3')
    """

    def __init__(
        self,
        access_key,
        secret_key,
        signature_version="s3v4",
        endpoint="s3.amazonaws.com",
    ):
        """
        Initialize the S3 authenticator.

        Args:
            access_key (str): Your S3 access key
            secret_key (str): Your S3 secret key
            signature_version (str): 's3v4' for Version 4 (default) or 's3' for Version 2
            endpoint (str): S3 endpoint hostname

        Raises:
            ValueError: If an unsupported signature version is specified
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.signature_version = signature_version
        self.endpoint = endpoint

        # Initialize the appropriate signature implementation
        if signature_version == "s3v4":
            self._signer = SignatureV4(access_key, secret_key, endpoint)
        elif signature_version == "s3":
            self._signer = SignatureV2(access_key, secret_key, endpoint)
        else:
            raise ValueError(
                "Unsupported signature version: {0}. Use 's3v4' or 's3'.".format(
                    signature_version
                )
            )

    def __call__(self, request):
        """
        Sign the request using the configured signature method.

        This method is called automatically by the requests library when
        the auth object is passed to a request.

        Args:
            request: The PreparedRequest object to sign

        Returns:
            The signed PreparedRequest object
        """
        # Ensure we have the required headers for signing
        self._prepare_request_headers(request)

        # Use the appropriate signer
        return self._signer.sign_request(request)

    def _prepare_request_headers(self, request):
        """
        Prepare request headers for signing.

        Ensures required headers are present and properly formatted.

        Args:
            request: The request object to prepare
        """
        # Ensure we have a date header
        if "Date" not in request.headers and "x-amz-date" not in request.headers:
            request.headers["Date"] = get_utc_datetime().strftime(
                "%a, %d %b %Y %H:%M:%S GMT"
            )

        # Fix content length for empty body requests
        if request.method in ["PUT", "POST"] and not hasattr(request, "body"):
            request.headers["Content-Length"] = "0"

        # Ensure host header is set correctly
        if "Host" not in request.headers:
            # Python 2/3 compatibility
            try:
                from urlparse import urlparse
            except ImportError:
                from urllib.parse import urlparse

            parsed = urlparse(request.url)
            request.headers["Host"] = parsed.netloc

    @property
    def region(self):
        """
        Get the AWS region for this authenticator.

        Returns:
            str: AWS region name
        """
        if hasattr(self._signer, "region"):
            return self._signer.region
        return "us-east-1"  # Default for Signature V2

    def __repr__(self):
        """String representation of the authenticator."""
        return "<S3Auth access_key={0} signature_version={1} endpoint={2}>".format(
            (
                self.access_key[:8] + "..."
                if len(self.access_key) > 8
                else self.access_key
            ),
            self.signature_version,
            self.endpoint,
        )
