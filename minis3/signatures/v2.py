# -*- coding: utf-8 -*-
"""
minis3.signatures.v2
~~~~~~~~~~~~~~~~~~~~

AWS Signature Version 2 implementation.

This is the legacy signature method, mainly used for compatibility
with older S3-compatible services.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import base64
import hashlib
import hmac
import re

# Python 2/3 support
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from ..util import stringify
from .base import BaseSignature

# A regexp used for detecting aws bucket names
BUCKET_VHOST_MATCH = re.compile(
    r"^([a-z0-9\-]+\.)?s3([a-z0-9\-]+)?\.amazonaws\.com$", flags=re.IGNORECASE
)

SUB_RESOURCE_KEYS = {
    "acl",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "requestPayment",
    "torrent",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
    "website",
}


class SignatureV2(BaseSignature):
    """
    AWS Signature Version 2 implementation.

    This is the legacy signature method, mainly used for compatibility
    with older S3-compatible services.
    """

    def sign_request(self, request):
        """
        Sign request using AWS Signature Version 2.

        Args:
            request: The request object to sign

        Returns:
            The signed request object with Authorization header
        """
        # Generate string to sign
        string_to_sign = self.string_to_sign(request)

        # Create signature
        signature = self._generate_signature(string_to_sign)

        # Add authorization header
        auth_header = "AWS {0}:{1}".format(self.access_key, signature)
        request.headers["Authorization"] = auth_header

        return request

    def string_to_sign(self, request):
        """
        Create the string to sign for Signature Version 2.

        Args:
            request: The request object

        Returns:
            str: String ready to be signed
        """
        # Parse the URL
        parsed_url = urlparse(request.url)

        # HTTP method
        method = request.method.upper()

        # Content MD5 (usually empty)
        content_md5 = request.headers.get("Content-MD5", "")
        # Content type
        content_type = request.headers.get("Content-Type", "")
        if not content_type:
            content_type = request.headers.get("content-type", "")
        # Date
        date = request.headers.get("Date", "")
        if "x-amz-date" in request.headers:
            date = ""  # Use empty string if x-amz-date is present

        # Canonicalized AMZ headers
        canonicalized_amz_headers = self._get_canonicalized_amz_headers(request.headers)

        # Canonicalized resource
        canonicalized_resource = self._get_canonicalized_resource(parsed_url)

        # Build string to sign
        string_to_sign = "\n".join(
            [
                method,
                content_md5,
                content_type,
                date,
                canonicalized_amz_headers + canonicalized_resource,
            ]
        )

        return string_to_sign

    def _generate_signature(self, string_to_sign):
        """
        Generate HMAC-SHA1 signature.

        Args:
            string_to_sign (str): The string to sign

        Returns:
            str: Base64 encoded signature
        """
        string_to_sign = stringify(string_to_sign)

        # Convert to bytes for Python 3 compatibility
        if not isinstance(string_to_sign, bytes):
            string_to_sign = string_to_sign.encode("utf-8")

        # Create HMAC-SHA1 signature
        digest = hmac.new(
            self.secret_key.encode("utf-8"), msg=string_to_sign, digestmod=hashlib.sha1
        ).digest()

        return base64.b64encode(digest).decode("ascii")

    def _get_canonicalized_amz_headers(self, headers):
        """
        Get canonicalized x-amz-* headers.

        Args:
            headers (dict): Request headers

        Returns:
            str: Canonicalized AMZ headers string
        """
        amz_headers = {}

        # Collect x-amz-* headers
        for key, value in headers.items():
            key_lower = key.lower()
            if key_lower.startswith("x-amz-"):
                # Handle multiple values by joining with comma
                if key_lower in amz_headers:
                    amz_headers[key_lower] += "," + str(value).strip()
                else:
                    amz_headers[key_lower] = str(value).strip()

        # Sort and format
        result = ""
        for key in sorted(amz_headers.keys()):
            result += "{0}:{1}\n".format(key, amz_headers[key])

        return result

    def _is_minio_or_local(self, hostname):
        """Check if hostname is a local development URL."""
        is_ip_address = hostname.replace(".", "").replace(":", "").isdigit()
        return (
            hostname.startswith("localhost")
            or hostname.startswith("127.0.0.1")
            or hostname.startswith("minio")
            or is_ip_address
        )

    def _get_subresource_query_string(self, query):
        """Extract sub-resource query parameters and format them."""
        if not query:
            return ""

        sub_resources = []
        query_params = query.split("&")

        for param in query_params:
            if "=" in param:
                key, value = param.split("=", 1)
            else:
                key, value = param, None

            if key in SUB_RESOURCE_KEYS:
                if value:
                    sub_resources.append("{0}={1}".format(key, value))
                else:
                    sub_resources.append(key)

        if sub_resources:
            return "?" + "&".join(sorted(sub_resources))

        return ""

    def _get_canonicalized_resource(self, parsed_url):
        """
        Get canonicalized resource string.

        Args:
            parsed_url: Parsed URL object

        Returns:
            str: Canonicalized resource string
        """
        # Start with the path
        resource = parsed_url.path or "/"
        hostname = parsed_url.hostname

        # Handle virtual hosted-style requests and custom hostnames
        if hostname:
            match = BUCKET_VHOST_MATCH.match(hostname)
            if match:
                # Extract bucket name if present from virtual host-style URL
                bucket = (match.groups()[0] or "").rstrip(".")
                if bucket:
                    resource = "/{0}{1}".format(bucket, resource)
            elif not self._is_minio_or_local(hostname):
                # It's a custom domain (virtual host), so prepend it
                resource = "/{0}{1}".format(hostname, resource)

        # Add query parameters that are sub-resources
        resource += self._get_subresource_query_string(parsed_url.query)

        return resource
