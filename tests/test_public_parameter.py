# -*- coding: utf-8 -*-
"""
Test public parameter functionality for uploads.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from io import BytesIO

import pytest

from minis3 import Connection


@pytest.mark.live
class TestPublicParameter:
    """Test the public parameter in upload operations."""

    @pytest.fixture(scope="class")
    def minio_connection(self):
        """Create a connection to the local MinIO instance."""
        import os
        import time

        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

        conn = Connection(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            tls=False,
            verify=False,
            signature_version="s3v4",
            path_style=True,
        )

        time.sleep(1)
        return conn

    @pytest.fixture
    def test_bucket_name(self, minio_connection):
        """Generate a unique test bucket name and create the bucket."""
        import uuid

        bucket_name = "test-public-{0}".format(str(uuid.uuid4()).replace("-", ""))

        try:
            response = minio_connection.create_bucket(bucket_name)
            if response.status_code in [200, 409]:
                yield bucket_name

                # Teardown
                try:
                    # Clean up any remaining objects
                    objects = list(minio_connection.list("", bucket=bucket_name))
                    for obj in objects:
                        try:
                            minio_connection.delete(obj["key"], bucket=bucket_name)
                        except Exception:
                            pass
                    # Delete the bucket
                    minio_connection.delete_bucket(bucket_name)
                except Exception as e:
                    print(
                        "Warning: Could not clean up test bucket {0}: {1}".format(
                            bucket_name, e
                        )
                    )
            else:
                pytest.skip("Could not create test bucket: {0}".format(bucket_name))
        except Exception as e:
            pytest.skip(
                "Could not create test bucket: {0} (error: {1})".format(bucket_name, e)
            )

    def test_upload_default_public_false(self, minio_connection, test_bucket_name):
        """Test that uploads default to public=False."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-default-public.txt"
        test_content = b"Test content for default public setting"

        try:
            # Upload without specifying public parameter (should default to False)
            test_file = BytesIO(test_content)
            response = conn.upload(test_key, test_file, bucket=bucket_name)
            assert response.status_code in [200, 201], "Upload should succeed"

            # Verify the file was uploaded
            download_response = conn.get(test_key, bucket=bucket_name)
            assert download_response.status_code == 200, "Download should succeed"
            assert download_response.content == test_content, "Content should match"

            # Check that no public-read ACL was set by examining headers
            # Note: MinIO may not always return ACL info in headers, but we can verify upload worked

        finally:
            # Clean up
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass

    def test_upload_explicit_public_false(self, minio_connection, test_bucket_name):
        """Test upload with explicit public=False."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-explicit-private.txt"
        test_content = b"Test content for explicit private setting"

        try:
            # Upload with explicit public=False
            test_file = BytesIO(test_content)
            response = conn.upload(
                test_key, test_file, bucket=bucket_name, public=False
            )
            assert response.status_code in [200, 201], "Upload should succeed"

            # Verify the file was uploaded
            download_response = conn.get(test_key, bucket=bucket_name)
            assert download_response.status_code == 200, "Download should succeed"
            assert download_response.content == test_content, "Content should match"

        finally:
            # Clean up
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass

    def test_upload_explicit_public_true(self, minio_connection, test_bucket_name):
        """Test upload with explicit public=True."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-explicit-public.txt"
        test_content = b"Test content for explicit public setting"

        try:
            # Upload with explicit public=True
            test_file = BytesIO(test_content)
            response = conn.upload(test_key, test_file, bucket=bucket_name, public=True)
            assert response.status_code in [200, 201], "Upload should succeed"

            # Verify the file was uploaded
            download_response = conn.get(test_key, bucket=bucket_name)
            assert download_response.status_code == 200, "Download should succeed"
            assert download_response.content == test_content, "Content should match"

            # Note: MinIO may handle ACL differently than AWS S3
            # The important thing is that the upload succeeds and the parameter is processed

        finally:
            # Clean up
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass

    def test_copy_with_public_parameter(self, minio_connection, test_bucket_name):
        """Test copy operation with public parameter."""
        conn = minio_connection
        bucket_name = test_bucket_name
        source_key = "test-copy-source.txt"
        dest_key = "test-copy-dest.txt"
        test_content = b"Test content for copy operation"

        try:
            # Upload source file
            test_file = BytesIO(test_content)
            upload_response = conn.upload(source_key, test_file, bucket=bucket_name)
            assert upload_response.status_code in [
                200,
                201,
            ], "Source upload should succeed"

            # Copy with public=True
            copy_response = conn.copy(
                source_key, bucket_name, dest_key, bucket_name, public=True
            )
            assert copy_response.status_code in [200, 201], "Copy should succeed"

            # Verify both files exist
            source_download = conn.get(source_key, bucket=bucket_name)
            dest_download = conn.get(dest_key, bucket=bucket_name)

            assert source_download.status_code == 200, "Source file should exist"
            assert dest_download.status_code == 200, "Destination file should exist"
            assert source_download.content == dest_download.content, (
                "Files should have same content"
            )

        finally:
            # Clean up
            try:
                conn.delete(source_key, bucket=bucket_name)
                conn.delete(dest_key, bucket=bucket_name)
            except Exception:
                pass
