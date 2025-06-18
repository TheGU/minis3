# -*- coding: utf-8 -*-
"""
Test URL format generation for different endpoint configurations.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from minis3.operations import S3Request


class TestURLFormats(unittest.TestCase):
    """Test URL format generation for different endpoint configurations."""

    def test_path_style_url_generation(self):
        """Test path-style URL generation for MinIO and custom endpoints."""

        # Mock connection with path-style enabled
        class MockConn:
            def __init__(self, endpoint, path_style=True):
                self.tls = False
                self.endpoint = endpoint
                self.path_style = path_style
                self.auth = None
                self.verify = True

        # Test MinIO localhost
        conn = MockConn("localhost:9000", path_style=True)
        request = S3Request(conn)
        url = request.bucket_url("test-file.txt", "my-bucket")
        expected = "http://localhost:9000/my-bucket/test-file.txt"
        self.assertEqual(url, expected)

        # Test custom endpoint with port
        conn = MockConn("minio.example.com:9000", path_style=True)
        request = S3Request(conn)
        url = request.bucket_url("documents/file.pdf", "storage-bucket")
        expected = "http://minio.example.com:9000/storage-bucket/documents/file.pdf"
        self.assertEqual(url, expected)

        # Test HTTPS with path style
        conn = MockConn("secure.minio.com", path_style=True)
        conn.tls = True
        request = S3Request(conn)
        url = request.bucket_url("secure-file.txt", "secure-bucket")
        expected = "https://secure.minio.com/secure-bucket/secure-file.txt"
        self.assertEqual(url, expected)

    def test_virtual_host_style_url_generation(self):
        """Test virtual host-style URL generation for AWS S3."""

        class MockConn:
            def __init__(self, endpoint, path_style=False):
                self.tls = True
                self.endpoint = endpoint
                self.path_style = path_style
                self.auth = None
                self.verify = True

        # Test AWS S3 standard
        conn = MockConn("s3.amazonaws.com", path_style=False)
        request = S3Request(conn)
        url = request.bucket_url("test-file.txt", "my-bucket")
        expected = "https://my-bucket.s3.amazonaws.com/test-file.txt"
        self.assertEqual(url, expected)

        # Test AWS S3 with region
        conn = MockConn("s3.us-west-2.amazonaws.com", path_style=False)
        request = S3Request(conn)
        url = request.bucket_url("data/report.csv", "analytics-bucket")
        expected = "https://analytics-bucket.s3.us-west-2.amazonaws.com/data/report.csv"
        self.assertEqual(url, expected)

        # Test custom domain (virtual host style)
        conn = MockConn("cdn.example.com", path_style=False)
        request = S3Request(conn)
        url = request.bucket_url("images/logo.png", "assets")
        expected = "https://assets.cdn.example.com/images/logo.png"
        self.assertEqual(url, expected)

    def test_bucket_operations_url_generation(self):
        """Test URL generation for bucket-level operations."""

        class MockConn:
            def __init__(self, endpoint, path_style=True):
                self.tls = False
                self.endpoint = endpoint
                self.path_style = path_style
                self.auth = None
                self.verify = True

        # Test bucket creation with path style
        conn = MockConn("localhost:9000", path_style=True)
        request = S3Request(conn)
        url = request.bucket_url("", "test-bucket")
        expected = "http://localhost:9000/test-bucket/"
        self.assertEqual(url, expected)

        # Test bucket listing with virtual host style
        conn = MockConn("s3.amazonaws.com", path_style=False)
        conn.tls = True
        request = S3Request(conn)
        url = request.bucket_url("", "data-bucket")
        expected = "https://data-bucket.s3.amazonaws.com/"
        self.assertEqual(url, expected)

    def test_key_with_special_characters(self):
        """Test URL generation with keys containing special characters."""

        class MockConn:
            def __init__(self, endpoint, path_style=True):
                self.tls = False
                self.endpoint = endpoint
                self.path_style = path_style
                self.auth = None
                self.verify = True

        conn = MockConn("localhost:9000", path_style=True)
        request = S3Request(conn)

        # Test key with leading slash (should be stripped)
        url = request.bucket_url("/path/to/file.txt", "my-bucket")
        expected = "http://localhost:9000/my-bucket/path/to/file.txt"
        self.assertEqual(url, expected)

        # Test key with spaces and special chars (will be handled by URL encoding elsewhere)
        url = request.bucket_url("folder/file with spaces.txt", "test-bucket")
        expected = "http://localhost:9000/test-bucket/folder/file with spaces.txt"
        self.assertEqual(url, expected)

    def test_query_parameters(self):
        """Test URL generation with query parameters."""

        class MockConn:
            def __init__(self, endpoint, path_style=True):
                self.tls = False
                self.endpoint = endpoint
                self.path_style = path_style
                self.auth = None
                self.verify = True

        # Test with query parameters
        conn = MockConn("localhost:9000", path_style=True)
        request = S3Request(conn, params={"uploadId": "12345", "partNumber": "1"})
        url = request.bucket_url("multipart-file.dat", "uploads-bucket")
        expected = "http://localhost:9000/uploads-bucket/multipart-file.dat?partNumber=1&uploadId=12345"
        self.assertEqual(url, expected)

        # Test with single parameter
        request = S3Request(conn, params={"acl": None})
        url = request.bucket_url("test-file.txt", "permissions-bucket")
        expected = "http://localhost:9000/permissions-bucket/test-file.txt?acl"
        self.assertEqual(url, expected)


if __name__ == "__main__":
    unittest.main()
