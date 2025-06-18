# -*- coding: utf-8 -*-
"""
minis3.operations.multipart_requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

S3 multipart upload operations.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os

# Python 2/3 compatibility for XML parsing
try:
    import lxml.etree as ET

    XML_PARSER = "lxml"
except ImportError:
    import xml.etree.ElementTree as ET

    XML_PARSER = "builtin"

from . import S3Request

# XML namespace helper
XML_PARSE_STRING = "{{http://s3.amazonaws.com/doc/2006-03-01/}}{0}"


class InitiateMultipartUploadRequest(S3Request):
    """
    Initiate a multipart upload.

    Creates a new multipart upload session for large file uploads
    that can be performed in parts.

    Args:
        conn: S3 connection object
        key (str): S3 object key for the upload
        bucket (str): S3 bucket name
    """

    def __init__(self, conn, key, bucket):
        params = {"uploads": None}
        super(InitiateMultipartUploadRequest, self).__init__(conn, params)
        self.key = key
        self.bucket = bucket

    def run(self, data=None):
        """
        Execute the initiate multipart upload request.

        Args:
            data: Optional data to send with the request

        Returns:
            str: Upload ID for the multipart upload session

        Raises:
            HTTPError: If the request fails
            ValueError: If the response cannot be parsed
        """
        url = self.bucket_url(self.key, self.bucket)
        response = self._make_request("POST", url)

        # Parse the upload ID from the XML response
        return self._extract_upload_id(response.content)

    def _extract_upload_id(self, xml_content):
        """
        Extract upload ID from the XML response.

        Args:
            xml_content (bytes): XML response content

        Returns:
            str: Upload ID

        Raises:
            ValueError: If upload ID cannot be extracted
        """
        try:
            k = XML_PARSE_STRING.format
            root = ET.fromstring(xml_content)
            upload_id_elem = root.find(k("UploadId"))

            if upload_id_elem is None or not upload_id_elem.text:
                raise ValueError("Upload ID not found in response")

            return upload_id_elem.text

        except ET.ParseError as e:
            raise ValueError("Failed to parse XML response: {0}".format(e))


class UploadPartRequest(S3Request):
    """
    Upload a single part of a multipart upload.

    Args:
        conn: S3 connection object
        key (str): S3 object key
        bucket (str): S3 bucket name
        fp: File-like object containing the part data
        part_num (int): Part number (1-based)
        upload_id (str): Multipart upload ID
        close (bool): Whether to close the file after upload
        rewind (bool): Whether to seek to beginning before upload
        headers (dict, optional): Additional HTTP headers
    """

    def __init__(
        self, conn, key, bucket, fp, part_num, upload_id, close, rewind, headers=None
    ):
        params = {"partNumber": part_num, "uploadId": upload_id}
        super(UploadPartRequest, self).__init__(conn, params)

        self.key = key
        self.bucket = bucket
        self.fp = fp
        self.part_num = part_num
        self.upload_id = upload_id
        self.headers = headers or {}
        self.close = close
        self.rewind = rewind

    def run(self):
        """
        Execute the upload part request.

        Returns:
            Response: HTTP response from the upload operation

        Note:
            The ETag header in the response is needed for completing
            the multipart upload.
        """
        # Prepare the file
        if self.rewind and hasattr(self.fp, "seek"):
            self.fp.seek(0, os.SEEK_SET)

        try:
            # Perform the upload
            url = self.bucket_url(self.key, self.bucket)
            return self._make_request("PUT", url, data=self.fp, headers=self.headers)

        finally:
            # Clean up if requested
            if self.close and hasattr(self.fp, "close"):
                self.fp.close()


class CompleteUploadRequest(S3Request):
    """
    Complete a multipart upload.

    Combines all uploaded parts into a single S3 object.

    Args:
        conn: S3 connection object
        key (str): S3 object key
        bucket (str): S3 bucket name
        upload_id (str): Multipart upload ID
        parts_list (list): List of part dictionaries with 'part_number' and 'etag'
    """

    def __init__(self, conn, key, bucket, upload_id, parts_list):
        params = {"uploadId": upload_id}
        super(CompleteUploadRequest, self).__init__(conn, params)

        self.key = key
        self.bucket = bucket
        self.upload_id = upload_id
        self.parts_list = parts_list

    def run(self):
        """
        Execute the complete multipart upload request.

        Returns:
            Response: HTTP response from the completion operation

        Raises:
            ValueError: If parts list is invalid
        """
        if not self.parts_list:
            raise ValueError("Parts list cannot be empty")

        # Build the XML payload
        xml_data = self._build_completion_xml()

        # Send the request
        url = self.bucket_url(self.key, self.bucket)
        return self._make_request("POST", url, data=xml_data)

    def _build_completion_xml(self):
        """
        Build the XML payload for completing the multipart upload.

        Returns:
            str: XML data for the completion request
        """
        xml_parts = ["<CompleteMultipartUpload>"]

        for part in self.parts_list:
            if "part_number" not in part or "etag" not in part:
                raise ValueError("Each part must have 'part_number' and 'etag'")

            xml_parts.extend(
                [
                    "<Part>",
                    "<PartNumber>{0}</PartNumber>".format(part["part_number"]),
                    "<ETag>{0}</ETag>".format(part["etag"]),
                    "</Part>",
                ]
            )

        xml_parts.append("</CompleteMultipartUpload>")
        return "".join(xml_parts)


class CancelUploadRequest(S3Request):
    """
    Cancel a multipart upload.

    Aborts the multipart upload and removes any uploaded parts.

    Args:
        conn: S3 connection object
        key (str): S3 object key
        bucket (str): S3 bucket name
        upload_id (str): Multipart upload ID to cancel
    """

    def __init__(self, conn, key, bucket, upload_id):
        params = {"uploadId": upload_id}
        super(CancelUploadRequest, self).__init__(conn, params)

        self.key = key
        self.bucket = bucket
        self.upload_id = upload_id

    def run(self):
        """
        Execute the cancel multipart upload request.

        Returns:
            Response: HTTP response from the cancellation operation
        """
        url = self.bucket_url(self.key, self.bucket)
        return self._make_request("DELETE", url)
