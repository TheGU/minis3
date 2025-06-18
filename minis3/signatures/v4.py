# -*- coding: utf-8 -*-
"""
minis3.signatures.v4
~~~~~~~~~~~~~~~~~~~~

AWS Signature Version 4 implementation.

This is the current AWS signature method, required for newer AWS regions
and recommended for all new implementations.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import hashlib
import hmac
from datetime import datetime

# Python 2/3 support
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from ..datetime_utils import get_utc_datetime
from .base import BaseSignature


class SignatureV4(BaseSignature):
    """
    AWS Signature Version 4 implementation.

    This is the current AWS signature method, required for newer AWS regions
    and recommended for all new implementations.
    """

    def __init__(
        self, access_key, secret_key, endpoint="s3.amazonaws.com", region=None
    ):
        """
        Initialize Signature Version 4.

        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
            endpoint (str): S3 endpoint hostname
            region (str): AWS region (auto-detected from endpoint if not provided)
        """
        super(SignatureV4, self).__init__(access_key, secret_key, endpoint)
        self.region = region or self._extract_region_from_endpoint(endpoint)
        self.service = "s3"

    def sign_request(self, request):
        """
        Sign request using AWS Signature Version 4.

        Args:
            request: The request object to sign

        Returns:
            The signed request object with Authorization header
        """
        # Ensure we have a date header - V4 prefers x-amz-date
        if "x-amz-date" not in request.headers:
            request.headers["x-amz-date"] = get_utc_datetime().strftime(
                "%Y%m%dT%H%M%SZ"
            )
            # Remove Date header if it exists, as x-amz-date takes precedence
            if "Date" in request.headers:
                del request.headers["Date"]

        # Get the timestamp for signing
        timestamp = self._get_timestamp_from_request(request)
        date_stamp = timestamp[:8]  # YYYYMMDD

        # Create the string to sign (before adding Authorization header)
        string_to_sign = self._create_string_to_sign(request, timestamp)

        # Calculate the signature
        signature = self._calculate_signature(string_to_sign, date_stamp)

        # Create the authorization header
        credential_scope = "{0}/{1}/{2}/aws4_request".format(
            date_stamp, self.region, self.service
        )
        signed_headers = self._get_signed_headers(request.headers)

        auth_header = "AWS4-HMAC-SHA256 Credential={0}/{1}, SignedHeaders={2}, Signature={3}".format(
            self.access_key, credential_scope, signed_headers, signature
        )

        request.headers["Authorization"] = auth_header
        return request

    def _extract_region_from_endpoint(self, endpoint):
        """
        Extract AWS region from endpoint.

        Args:
            endpoint (str): S3 endpoint

        Returns:
            str: AWS region
        """
        if "s3.amazonaws.com" in endpoint:
            return "us-east-1"  # Default region
        elif "s3-" in endpoint and ".amazonaws.com" in endpoint:
            # Extract region from s3-region.amazonaws.com format
            return endpoint.split("s3-")[1].split(".amazonaws.com")[0]
        elif ".s3." in endpoint and ".amazonaws.com" in endpoint:
            # Extract region from bucket.s3.region.amazonaws.com format
            return endpoint.split(".s3.")[1].split(".amazonaws.com")[0]
        else:
            # For non-AWS endpoints, use us-east-1 as default
            return "us-east-1"

    def _get_timestamp_from_request(self, request):
        """
        Get timestamp from request headers.

        Args:
            request: The request object

        Returns:
            str: Timestamp in ISO format
        """
        if "x-amz-date" in request.headers:
            return request.headers["x-amz-date"]
        elif "Date" in request.headers:
            # Convert Date header to x-amz-date format
            date_str = request.headers["Date"]
            # Try different date formats
            try:
                # Try RFC 2822 format with GMT
                date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S GMT")
            except ValueError:
                try:
                    # Try RFC 2822 format with timezone offset (+0000)
                    date_obj = datetime.strptime(
                        date_str, "%a, %d %b %Y %H:%M:%S +0000"
                    )
                except ValueError:
                    try:
                        # Try without timezone
                        date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S")
                    except ValueError:
                        # If all else fails, use current time
                        date_obj = get_utc_datetime()
            return date_obj.strftime("%Y%m%dT%H%M%SZ")
        else:
            return get_utc_datetime().strftime("%Y%m%dT%H%M%SZ")

    def _create_string_to_sign(self, request, timestamp):
        """
        Create the string to sign for Signature Version 4.

        Args:
            request: The request object
            timestamp (str): Request timestamp

        Returns:
            str: String to sign
        """
        date_stamp = timestamp[:8]
        credential_scope = "{0}/{1}/{2}/aws4_request".format(
            date_stamp, self.region, self.service
        )
        canonical_request = self._create_canonical_request(request)
        canonical_request_hash = hashlib.sha256(
            canonical_request.encode("utf-8")
        ).hexdigest()

        string_to_sign = "\n".join(
            ["AWS4-HMAC-SHA256", timestamp, credential_scope, canonical_request_hash]
        )

        return string_to_sign

    def _create_canonical_request(self, request):
        """
        Create canonical request for Signature Version 4.

        Args:
            request: The request object

        Returns:
            str: Canonical request string
        """
        parsed_url = urlparse(request.url)

        # HTTP method
        method = request.method.upper()

        # Canonical URI
        canonical_uri = parsed_url.path or "/"

        # Canonical query string
        canonical_query_string = self._get_canonical_query_string(parsed_url.query)

        # Canonical headers
        canonical_headers = self._get_canonical_headers(request.headers)

        # Signed headers
        signed_headers = self._get_signed_headers(request.headers)

        # Payload hash
        payload_hash = self._get_payload_hash(request)

        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query_string,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )

        return canonical_request

    def _get_canonical_query_string(self, query_string):
        """Get canonical query string."""
        if not query_string:
            return ""

        # Parse and sort query parameters
        params = []
        for param in query_string.split("&"):
            if "=" in param:
                key, value = param.split("=", 1)
                params.append((key, value))
            else:
                params.append((param, ""))
        # Sort by key and format
        params.sort()
        return "&".join("=".join(p) for p in params)

    def _get_canonical_headers(self, headers):
        """Get canonical headers string."""
        canonical_headers = {}

        for key, value in headers.items():
            key_lower = key.lower()
            # Skip Authorization header - it should not be included in canonical request
            if key_lower == "authorization":
                continue
            canonical_headers[key_lower] = str(value).strip()

        result = ""
        for key in sorted(canonical_headers.keys()):
            result += "{0}:{1}\n".format(key, canonical_headers[key])

        return result

    def _get_signed_headers(self, headers):
        """Get signed headers string."""
        signed_headers = []
        for key in headers.keys():
            key_lower = key.lower()
            # Skip Authorization header - it should not be included in signed headers
            if key_lower == "authorization":
                continue
            signed_headers.append(key_lower)
        signed_headers.sort()
        return ";".join(signed_headers)

    def _get_payload_hash(self, request):
        """Get payload hash for the request."""
        # For S3, we need to calculate SHA256 hash of the payload
        # For bucket creation and most S3 operations, use actual hash instead of UNSIGNED-PAYLOAD
        if hasattr(request, "body") and request.body:
            if isinstance(request.body, bytes):
                return hashlib.sha256(request.body).hexdigest()
            elif isinstance(request.body, str):
                return hashlib.sha256(request.body.encode("utf-8")).hexdigest()

        # For empty payload, calculate hash of empty string
        return hashlib.sha256(b"").hexdigest()

    def _calculate_signature(self, string_to_sign, date_stamp):
        """
        Calculate the signature using the signing key.

        Args:
            string_to_sign (str): The string to sign
            date_stamp (str): Date stamp (YYYYMMDD)

        Returns:
            str: Hex-encoded signature
        """

        def _sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        # Create the signing key
        k_date = _sign(("AWS4" + self.secret_key).encode("utf-8"), date_stamp)
        k_region = _sign(k_date, self.region)
        k_service = _sign(k_region, self.service)
        k_signing = _sign(k_service, "aws4_request")

        # Sign the string
        signature = hmac.new(
            k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return signature
