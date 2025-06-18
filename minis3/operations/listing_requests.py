# -*- coding: utf-8 -*-
"""
minis3.operations.listing_requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

S3 listing operations (list objects, list multipart uploads, etc.)
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import datetime

# Python 2/3 compatibility for XML parsing
try:
    import lxml.etree as ET

    XML_PARSER = "lxml"
except ImportError:
    import xml.etree.ElementTree as ET

    XML_PARSER = "builtin"

from ..util import stringify
from . import S3Request

# XML namespace helper
XML_PARSE_STRING = "{{http://s3.amazonaws.com/doc/2006-03-01/}}{0}"


class ListRequest(S3Request):
    """
    List objects in an S3 bucket.

    Provides an iterator interface for efficiently listing objects,
    handling pagination automatically.

    Args:
        conn: S3 connection object
        prefix (str): Only list objects with this prefix
        bucket (str): S3 bucket name
    """

    def __init__(self, conn, prefix, bucket):
        super(ListRequest, self).__init__(conn)

        # Handle prefix encoding for Python 2/3 compatibility
        if prefix and not isinstance(prefix, str):
            prefix = prefix.encode("utf-8")

        self.prefix = prefix
        self.bucket = bucket

    def run(self):
        """
        Execute the list request.

        Returns:
            iterator: Iterator over object metadata dictionaries
        """
        return iter(self)

    def __iter__(self):
        """
        Iterate over all objects in the bucket with the given prefix.

        Yields:
            dict: Object metadata with keys: 'key', 'size', 'last_modified',
                  'etag', 'storage_class'
        """
        marker = ""
        more = True
        url = self.bucket_url("", self.bucket)
        k = XML_PARSE_STRING.format

        while more:
            # Make the list request
            response = self._make_request(
                "GET",
                url,
                params={
                    "prefix": self.prefix,
                    "marker": marker,
                },
            )

            # Parse XML response
            root = ET.fromstring(response.content)

            # Extract object information
            for tag in root.findall(k("Contents")):
                obj_info = self._extract_object_info(tag, k)
                if obj_info:
                    yield obj_info
                    marker = obj_info["key"]  # Update marker for next page

            # Check if there are more results
            truncated_element = root.find(k("IsTruncated"))
            more = truncated_element is not None and truncated_element.text == "true"

    def _extract_object_info(self, tag, k):
        """
        Extract object information from XML element.

        Args:
            tag: XML element containing object data
            k: XML namespace formatter function

        Returns:
            dict: Object metadata or None if parsing fails
        """
        try:
            key_elem = tag.find(k("Key"))
            size_elem = tag.find(k("Size"))
            modified_elem = tag.find(k("LastModified"))
            etag_elem = tag.find(k("ETag"))
            storage_elem = tag.find(k("StorageClass"))
            # Ensure all required elements are present
            if not all(
                [
                    key_elem is not None,
                    size_elem is not None,
                    modified_elem is not None,
                    etag_elem is not None,
                ]
            ):
                return None

            return {
                "key": key_elem.text,
                "size": int(size_elem.text),
                "last_modified": datetime.datetime.strptime(
                    modified_elem.text, "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "etag": etag_elem.text[1:-1],  # Remove quotes
                "storage_class": (
                    storage_elem.text if storage_elem is not None else "STANDARD"
                ),
            }
        except (ValueError, AttributeError, TypeError):
            # Log the error in a real implementation
            return None


class ListMultipartUploadRequest(S3Request):
    """
    List active multipart uploads in a bucket.

    Args:
        conn: S3 connection object
        prefix (str): Only list uploads with this prefix
        bucket (str): S3 bucket name
        max_uploads (int): Maximum number of uploads to return per request
        encoding (str): Encoding type for the response
        key_marker (str): Start listing after this key
        upload_id_marker (str): Start listing after this upload ID
    """

    def __init__(
        self, conn, prefix, bucket, max_uploads, encoding, key_marker, upload_id_marker
    ):
        params = {"uploads": None}
        super(ListMultipartUploadRequest, self).__init__(conn, params)

        self.conn = conn
        self.prefix = stringify(prefix)
        self.bucket = bucket
        self.max_uploads = max_uploads
        self.encoding = encoding
        self.key_marker = key_marker
        self.upload_id_marker = upload_id_marker

    def run(self):
        """
        Execute the list multipart uploads request.

        Returns:
            iterator: Iterator over MultipartUpload objects
        """
        return iter(self)

    def __iter__(self):
        """
        Iterate over active multipart uploads.

        Yields:
            MultipartUpload: Upload objects with key and uploadId attributes
        """
        more = True
        url = self.bucket_url("", self.bucket)
        k = XML_PARSE_STRING.format

        while more:
            # Make the request
            response = self._make_request(
                "GET",
                url,
                params={
                    "uploads": None,
                    "encoding-type": self.encoding,
                    "max-uploads": self.max_uploads,
                    "key-marker": self.key_marker,
                    "prefix": self.prefix,
                    "upload-id-marker": self.upload_id_marker,
                },
            )

            # Parse XML response
            root = ET.fromstring(response.content)

            # Extract upload information
            for tag in root.findall(k("Upload")):
                upload = self._extract_upload_info(tag, k)
                if upload:
                    yield upload

            # Check pagination
            truncated_element = root.find(k("IsTruncated"))
            more = truncated_element is not None and truncated_element.text == "true"

            if more:
                # Update markers for next page
                next_key = root.find(k("NextKeyMarker"))
                next_upload_id = root.find(k("NextUploadIdMarker"))

                if next_key is not None:
                    self.key_marker = next_key.text
                if next_upload_id is not None:
                    self.upload_id_marker = next_upload_id.text

    def _extract_upload_info(self, tag, k):
        """
        Extract multipart upload information from XML element.

        Args:
            tag: XML element containing upload data
            k: XML namespace formatter function

        Returns:
            MultipartUpload: Upload object or None if parsing fails
        """
        try:
            key_elem = tag.find(k("Key"))
            upload_id_elem = tag.find(k("UploadId"))

            if not all([key_elem is not None, upload_id_elem is not None]):
                return None

            # Import here to avoid circular imports
            from ..multipart_upload import MultipartUpload

            upload = MultipartUpload(self.conn, self.bucket, key_elem.text)
            upload.uploadId = upload_id_elem.text
            return upload

        except (AttributeError, TypeError):
            return None


class ListPartsRequest(S3Request):
    """
    List parts of a multipart upload.

    Args:
        conn: S3 connection object
        key (str): Object key
        bucket (str): S3 bucket name
        upload_id (str): Multipart upload ID
        max_parts (int): Maximum parts to return per request
        encoding (str): Encoding type for response
        part_number_marker (str): Start listing after this part number
    """

    def __init__(
        self, conn, key, bucket, upload_id, max_parts, encoding, part_number_marker
    ):
        params = {"uploadId": upload_id}
        super(ListPartsRequest, self).__init__(conn, params)

        self.key = key
        self.bucket = bucket
        self.upload_id = upload_id
        self.max_parts = max_parts
        self.encoding = encoding
        self.part_number_marker = part_number_marker

    def run(self):
        """
        Execute the list parts request.

        Returns:
            iterator: Iterator over part information dictionaries
        """
        return iter(self)

    def __iter__(self):
        """
        Iterate over parts of the multipart upload.

        Yields:
            dict: Part metadata with keys: 'part_number', 'last_modified',
                  'etag', 'size'
        """
        more = True
        url = self.bucket_url(self.key, self.bucket)
        k = XML_PARSE_STRING.format

        while more:
            # Make the request
            response = self._make_request(
                "GET",
                url,
                params={
                    "uploadId": self.upload_id,
                    "encoding-type": self.encoding,
                    "max-parts": self.max_parts,
                    "part-number-marker": self.part_number_marker,
                },
            )

            # Parse XML response
            root = ET.fromstring(response.content)

            # Extract part information
            for tag in root.findall(k("Part")):
                part_info = self._extract_part_info(tag, k)
                if part_info:
                    yield part_info

            # Check pagination
            truncated_element = root.find(k("IsTruncated"))
            more = truncated_element is not None and truncated_element.text == "true"

            if more:
                # Update marker for next page
                next_marker = root.find(k("NextPartNumberMarker"))
                if next_marker is not None:
                    self.part_number_marker = next_marker.text

    def _extract_part_info(self, tag, k):
        """
        Extract part information from XML element.

        Args:
            tag: XML element containing part data
            k: XML namespace formatter function

        Returns:
            dict: Part metadata or None if parsing fails
        """
        try:
            part_num_elem = tag.find(k("PartNumber"))
            modified_elem = tag.find(k("LastModified"))
            etag_elem = tag.find(k("ETag"))
            size_elem = tag.find(k("Size"))

            if not all(
                [
                    part_num_elem is not None,
                    modified_elem is not None,
                    etag_elem is not None,
                    size_elem is not None,
                ]
            ):
                return None

            return {
                "part_number": int(part_num_elem.text),
                "last_modified": modified_elem.text,
                "etag": etag_elem.text,
                "size": int(size_elem.text),
            }

        except (ValueError, AttributeError, TypeError):
            return None
