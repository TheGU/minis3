# -*- coding: utf-8 -*-
"""
minis3.signatures.base
~~~~~~~~~~~~~~~~~~~~~~

Base class for AWS signature implementations.
"""

from __future__ import absolute_import, division, print_function, unicode_literals


class BaseSignature(object):
    """Base class for AWS signature implementations."""

    def __init__(self, access_key, secret_key, endpoint="s3.amazonaws.com"):
        """
        Initialize the signature implementation.

        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
            endpoint (str): S3 endpoint hostname
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint

    def sign_request(self, request):
        """
        Sign the given request.

        Args:
            request: The request object to sign

        Returns:
            The signed request object
        """
        raise NotImplementedError("Subclasses must implement sign_request")
