# -*- coding: utf-8 -*-
"""
minis3.request_factory
~~~~~~~~~~~~~~~~~~~~~~

Factory module for S3 request objects.

This module provides a clean interface to all S3 request types,
organized by functionality and well-documented for easy use.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

# Import all request types
from .operations import S3Request
from .operations.listing_requests import (
    ListMultipartUploadRequest,
    ListPartsRequest,
    ListRequest,
)
from .operations.multipart_requests import (
    CancelUploadRequest,
    CompleteUploadRequest,
    InitiateMultipartUploadRequest,
    UploadPartRequest,
)
from .operations.object_requests import (
    CopyRequest,
    DeleteRequest,
    GetRequest,
    HeadRequest,
    UpdateMetadataRequest,
    UploadRequest,
)

# Export all request classes for backward compatibility
__all__ = [
    "S3Request",
    "GetRequest",
    "UploadRequest",
    "DeleteRequest",
    "CopyRequest",
    "UpdateMetadataRequest",
    "HeadRequest",
    "ListRequest",
    "ListMultipartUploadRequest",
    "ListPartsRequest",
    "InitiateMultipartUploadRequest",
    "UploadPartRequest",
    "CompleteUploadRequest",
    "CancelUploadRequest",
]


def create_request(request_type, *args, **kwargs):
    """
    Factory function to create request objects by type.

    This provides a convenient way to create request objects
    without importing specific classes.

    Args:
        request_type (str): Type of request to create
        *args: Arguments to pass to the request constructor
        **kwargs: Keyword arguments to pass to the request constructor

    Returns:
        S3Request: The appropriate request object

    Raises:
        ValueError: If request_type is not recognized

    Examples:
        >>> request = create_request('upload', conn, 'key', file_obj, 'bucket')
        >>> request = create_request('get', conn, 'key', 'bucket')
    """
    request_types = {
        "get": GetRequest,
        "upload": UploadRequest,
        "delete": DeleteRequest,
        "copy": CopyRequest,
        "update_metadata": UpdateMetadataRequest,
        "head": HeadRequest,
        "list": ListRequest,
        "list_multipart_uploads": ListMultipartUploadRequest,
        "list_parts": ListPartsRequest,
        "initiate_multipart_upload": InitiateMultipartUploadRequest,
        "upload_part": UploadPartRequest,
        "complete_upload": CompleteUploadRequest,
        "cancel_upload": CancelUploadRequest,
    }

    if request_type not in request_types:
        raise ValueError(
            "Unknown request type: {0}. Available types: {1}".format(
                request_type, ", ".join(sorted(request_types.keys()))
            )
        )

    request_class = request_types[request_type]
    return request_class(*args, **kwargs)
