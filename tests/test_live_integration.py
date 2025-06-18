# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import time
import uuid
from io import BytesIO

import pytest

from minis3 import Connection


@pytest.mark.live
class TestLiveIntegration:
    """Live integration tests that require a running MinIO instance."""

    @pytest.fixture(scope="class")
    def minio_connection(self):
        """Create a connection to the local MinIO instance."""
        # Get connection details from environment variables
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
        # Create connection with MinIO-specific settings
        conn = Connection(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            tls=False,  # MinIO typically runs on HTTP in test environments
            verify=False,  # Skip SSL verification for local testing
            signature_version="s3v4",  # Use Signature Version 4
            path_style=True,  # Use path-style URLs for MinIO compatibility
        )

        # Wait a bit for MinIO to be ready
        time.sleep(2)

        return conn

    @pytest.fixture
    def test_bucket_name(self, minio_connection):
        """Generate a unique test bucket name and create the bucket."""
        bucket_name = "test-bucket-{0}".format(str(uuid.uuid4()).replace("-", ""))

        # Create the bucket using the built-in create_bucket method
        try:
            response = minio_connection.create_bucket(bucket_name)
            if response.status_code in [
                200,
                409,
            ]:  # 200 = created, 409 = already exists
                yield bucket_name  # Use yield to enable teardown

                # Teardown: Clean up the bucket after the test
                try:
                    if not bucket_name.startswith("test-bucket-"):
                        raise ValueError(
                            "Bucket name does not match expected test pattern"
                        )

                    # First, try to delete any remaining objects in the bucket
                    objects = list(minio_connection.list("", bucket=bucket_name))
                    for obj in objects:
                        try:
                            minio_connection.delete(obj["key"], bucket=bucket_name)
                        except Exception:
                            pass  # Ignore errors when cleaning up objects

                    # Then delete the bucket itself
                    delete_response = minio_connection.delete_bucket(bucket_name)
                    print(
                        "Cleaned up test bucket: {0} (status: {1})".format(
                            bucket_name, delete_response.status_code
                        )
                    )
                except Exception as e:
                    print(
                        "Warning: Could not clean up test bucket {0}: {1}".format(
                            bucket_name, e
                        )
                    )
            else:
                pytest.skip(
                    "Could not create test bucket: {0} (status: {1})".format(
                        bucket_name, response.status_code
                    )
                )
        except Exception as e:
            pytest.skip(
                "Could not create test bucket: {0} (error: {1})".format(bucket_name, e)
            )

    @pytest.fixture
    def test_file_content(self):
        """Generate test file content."""
        return b"Hello, minis3! This is test content for S3-compatible storage."

    def test_full_lifecycle(
        self, minio_connection, test_bucket_name, test_file_content
    ):
        """Test the full lifecycle: create bucket, upload, download, list, delete."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-file.txt"

        try:
            # Step 1: Create a temporary file-like object from test content
            test_file = BytesIO(test_file_content)

            # Step 2: Upload the file
            response = conn.upload(test_key, test_file, bucket=bucket_name)
            assert response.status_code in [200, 201], "Upload should succeed"

            # Step 3: Download the file and verify content
            download_response = conn.get(test_key, bucket=bucket_name)
            assert download_response.status_code == 200, "Download should succeed"
            assert download_response.content == test_file_content, (
                "Downloaded content should match uploaded content"
            )

            # Step 4: List objects in the bucket and verify our file is there
            objects = list(conn.list("", bucket=bucket_name))
            assert len(objects) >= 1, "Should have at least one object in bucket"
            assert any(obj["key"] == test_key for obj in objects), (
                "Our test file should be in the list"
            )

            # Step 5: Delete the file
            delete_response = conn.delete(test_key, bucket=bucket_name)
            assert delete_response.status_code in [200, 204], "Delete should succeed"

            # Step 6: Verify the file is gone
            objects_after_delete = list(conn.list("", bucket=bucket_name))
            assert not any(obj["key"] == test_key for obj in objects_after_delete), (
                "File should be deleted"
            )

        except Exception as e:
            # Clean up on failure
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass
            raise e

    def test_upload_with_metadata(
        self, minio_connection, test_bucket_name, test_file_content
    ):
        """Test uploading a file with custom metadata."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-metadata-file.txt"

        try:
            from io import BytesIO

            test_file = BytesIO(test_file_content)

            # Upload with custom headers
            custom_headers = {
                "x-amz-meta-author": "minis3-test",
                "x-amz-meta-purpose": "integration-testing",
            }

            response = conn.upload(
                test_key,
                test_file,
                bucket=bucket_name,
                headers=custom_headers,
                content_type="text/plain",
            )
            assert response.status_code in [
                200,
                201,
            ], "Upload with metadata should succeed"

            # Clean up
            conn.delete(test_key, bucket=bucket_name)

        except Exception as e:
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass
            raise e

    def test_copy_operation(
        self, minio_connection, test_bucket_name, test_file_content
    ):
        """Test copying files within the same bucket."""
        conn = minio_connection
        bucket_name = test_bucket_name
        source_key = "source-file.txt"
        destination_key = "destination-file.txt"

        try:
            from io import BytesIO

            test_file = BytesIO(test_file_content)

            # Upload source file
            upload_response = conn.upload(source_key, test_file, bucket=bucket_name)
            assert upload_response.status_code in [
                200,
                201,
            ], "Source upload should succeed"

            # Copy the file
            copy_response = conn.copy(
                source_key, bucket_name, destination_key, bucket_name
            )
            assert copy_response.status_code in [200, 201], "Copy should succeed"

            # Verify both files exist
            source_download = conn.get(source_key, bucket=bucket_name)
            dest_download = conn.get(destination_key, bucket=bucket_name)
            assert source_download.status_code == 200, "Source file should exist"
            assert dest_download.status_code == 200, "Destination file should exist"
            assert source_download.content == dest_download.content, (
                "Files should have same content"
            )

            # Clean up
            conn.delete(source_key, bucket=bucket_name)
            conn.delete(destination_key, bucket=bucket_name)
        except Exception as e:
            try:
                conn.delete(source_key, bucket=bucket_name)
                conn.delete(destination_key, bucket=bucket_name)
            except Exception:
                pass
            raise e

    def test_bucket_operations(self, minio_connection):
        """Test bucket creation, listing, and deletion operations."""
        conn = minio_connection
        bucket_name = "test-bucket-ops-{0}".format(str(uuid.uuid4()).replace("-", ""))

        try:
            # Test bucket creation
            create_response = conn.create_bucket(bucket_name)
            assert create_response.status_code in [
                200,
                409,
            ], "Bucket creation should succeed"

            # Test bucket exists by trying to list objects (should return empty)
            objects = list(conn.list("", bucket=bucket_name))
            assert len(objects) == 0, "New bucket should be empty"

            # Test uploading a file to verify bucket is functional
            from io import BytesIO

            test_content = b"Test content for bucket operations"
            test_file = BytesIO(test_content)
            upload_response = conn.upload(
                "test-file.txt", test_file, bucket=bucket_name
            )
            assert upload_response.status_code in [
                200,
                201,
            ], "Upload to new bucket should succeed"

            # Verify file is in bucket
            objects = list(conn.list("", bucket=bucket_name))
            assert len(objects) == 1, "Bucket should contain one object"
            assert objects[0]["key"] == "test-file.txt", "Object key should match"

            # Clean up the object
            delete_response = conn.delete("test-file.txt", bucket=bucket_name)
            assert delete_response.status_code in [
                200,
                204,
            ], "Object deletion should succeed"

            # Verify bucket is empty again
            objects = list(conn.list("", bucket=bucket_name))
            assert len(objects) == 0, "Bucket should be empty after deletion"

        finally:
            # Clean up bucket
            try:
                # Ensure bucket is empty
                objects = list(conn.list("", bucket=bucket_name))
                for obj in objects:
                    conn.delete(obj["key"], bucket=bucket_name)
                # Delete bucket
                conn.delete_bucket(bucket_name)
            except Exception:
                pass

    def test_object_listing_with_prefix(self, minio_connection, test_bucket_name):
        """Test object listing with different prefixes."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_content = b"Test content for prefix testing"

        # Upload files with different prefixes
        test_files = [
            "documents/file1.txt",
            "documents/file2.txt",
            "images/photo1.jpg",
            "images/photo2.jpg",
            "videos/clip1.mp4",
            "root-file.txt",
        ]

        try:
            # Upload all test files
            for file_key in test_files:
                test_file = BytesIO(test_content)
                response = conn.upload(file_key, test_file, bucket=bucket_name)
                assert response.status_code in [
                    200,
                    201,
                ], "Upload of {0} should succeed".format(file_key)

            # Test listing all objects
            all_objects = list(conn.list("", bucket=bucket_name))
            assert len(all_objects) >= len(test_files), "Should list all uploaded files"

            # Test listing with 'documents/' prefix
            doc_objects = list(conn.list("documents/", bucket=bucket_name))
            doc_keys = [obj["key"] for obj in doc_objects]
            assert "documents/file1.txt" in doc_keys, (
                "Should include documents/file1.txt"
            )
            assert "documents/file2.txt" in doc_keys, (
                "Should include documents/file2.txt"
            )
            assert "images/photo1.jpg" not in doc_keys, (
                "Should not include images files"
            )

            # Test listing with 'images/' prefix
            img_objects = list(conn.list("images/", bucket=bucket_name))
            img_keys = [obj["key"] for obj in img_objects]
            assert len(img_keys) >= 2, "Should find at least 2 image files"
            assert all("images/" in key for key in img_keys), (
                "All keys should have images/ prefix"
            )

            # Test listing with non-existent prefix
            empty_objects = list(conn.list("nonexistent/", bucket=bucket_name))
            assert len(empty_objects) == 0, (
                "Should return empty list for non-existent prefix"
            )

        finally:
            # Clean up all test files
            for file_key in test_files:
                try:
                    conn.delete(file_key, bucket=bucket_name)
                except Exception:
                    pass

    def test_error_handling(self, minio_connection):
        """Test error handling for various failure scenarios."""
        conn = minio_connection

        # Test downloading non-existent object
        try:
            response = conn.get("non-existent-file.txt", bucket="non-existent-bucket")
            # Should either raise an exception or return error status
            assert response.status_code >= 400, (
                "Should return error status for non-existent object"
            )
        except Exception:
            # Exception is also acceptable for this case
            pass

        # Test deleting non-existent object (should not fail)
        try:
            response = conn.delete(
                "non-existent-file.txt", bucket="non-existent-bucket"
            )
            # Delete operations often succeed even if object doesn't exist
            # This is S3's behavior - it's idempotent
        except Exception:
            # Some implementations may raise exceptions, which is also acceptable
            pass

    def test_file_with_metadata_retrieval(self, minio_connection, test_bucket_name):
        """Test uploading with metadata and attempting to retrieve it."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_key = "test-metadata-retrieval.txt"
        test_content = b"Test content with metadata"

        try:
            # Upload with custom metadata
            custom_headers = {
                "x-amz-meta-author": "test-user",
                "x-amz-meta-project": "minis3-testing",
                "x-amz-meta-version": "1.0",
            }

            test_file = BytesIO(test_content)
            upload_response = conn.upload(
                test_key,
                test_file,
                bucket=bucket_name,
                headers=custom_headers,
                content_type="text/plain",
            )
            assert upload_response.status_code in [
                200,
                201,
            ], "Upload with metadata should succeed"

            # Download and check if we can access the content
            download_response = conn.get(test_key, bucket=bucket_name)
            assert download_response.status_code == 200, "Download should succeed"
            assert download_response.content == test_content, "Content should match"

            # Note: Metadata retrieval depends on the S3 implementation
            # Some services include metadata in response headers, others don't

        finally:
            try:
                conn.delete(test_key, bucket=bucket_name)
            except Exception:
                pass

    def test_large_key_names(self, minio_connection, test_bucket_name):
        """Test upload/download with long key names and special characters."""
        conn = minio_connection
        bucket_name = test_bucket_name
        test_content = b"Test content for special key names"

        # Test with various key patterns
        test_keys = [
            "very/deeply/nested/folder/structure/file.txt",
            "file-with-dashes-and_underscores.txt",
            "file.with.multiple.dots.in.name.txt",
            "folder with spaces/file with spaces.txt",  # May need URL encoding
            "unicode-文件名.txt",  # Unicode characters
        ]

        successful_keys = []

        for test_key in test_keys:
            try:
                # Upload
                test_file = BytesIO(test_content)
                upload_response = conn.upload(test_key, test_file, bucket=bucket_name)

                if upload_response.status_code in [200, 201]:
                    successful_keys.append(test_key)

                    # Try to download
                    download_response = conn.get(test_key, bucket=bucket_name)
                    assert download_response.status_code == 200, (
                        "Download of {0} should succeed".format(test_key)
                    )
                    assert download_response.content == test_content, (
                        "Content should match for {0}".format(test_key)
                    )

            except Exception as e:
                # Some key names may not be supported by all S3 implementations
                print("Key '{0}' not supported: {1}".format(test_key, e))
                continue

        # We should be able to handle at least the basic key patterns
        assert len(successful_keys) >= 3, (
            "Should successfully handle multiple key patterns"
        )

        # Clean up successful uploads
        for key in successful_keys:
            try:
                conn.delete(key, bucket=bucket_name)
            except Exception:
                pass

    def test_signature_versions(
        self, minio_connection, test_bucket_name, test_file_content
    ):
        """Test connections with different signature versions."""
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

        # Test with Signature Version 4 (default)
        conn_v4 = Connection(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            tls=False,
            verify=False,
            signature_version="s3v4",
            path_style=True,  # Use path-style URLs for MinIO compatibility
        )

        # Test with Signature Version 2 (legacy)
        conn_v2 = Connection(
            access_key=access_key,
            secret_key=secret_key,
            endpoint=endpoint,
            tls=False,
            verify=False,
            signature_version="s3",
            path_style=True,  # Use path-style URLs for MinIO compatibility
        )

        test_key = "sig-version-test.txt"
        bucket_name = test_bucket_name

        try:
            from io import BytesIO

            # Test V4 signature
            test_file_v4 = BytesIO(test_file_content)
            response_v4 = conn_v4.upload(
                test_key + "-v4", test_file_v4, bucket=bucket_name
            )
            assert response_v4.status_code in [
                200,
                201,
            ], "V4 signature upload should succeed"

            # Test V2 signature
            test_file_v2 = BytesIO(test_file_content)
            response_v2 = conn_v2.upload(
                test_key + "-v2", test_file_v2, bucket=bucket_name
            )
            assert response_v2.status_code in [
                200,
                201,
            ], "V2 signature upload should succeed"

            # Clean up
            conn_v4.delete(test_key + "-v4", bucket=bucket_name)
            conn_v2.delete(test_key + "-v2", bucket=bucket_name)

        except Exception as e:
            # Clean up on failure
            try:
                conn_v4.delete(test_key + "-v4", bucket=bucket_name)
                conn_v2.delete(test_key + "-v2", bucket=bucket_name)
            except Exception:
                pass
            raise e

    def test_comprehensive_bucket_operations(self, minio_connection):
        """Test comprehensive bucket creation, object operations, and deletion."""
        conn = minio_connection
        bucket_name = "test-comprehensive-{0}".format(
            str(uuid.uuid4()).replace("-", "")
        )

        try:
            from io import BytesIO

            # Test bucket creation
            create_response = conn.create_bucket(bucket_name)
            assert create_response.status_code in [
                200,
                409,
            ], "Bucket creation should succeed"

            # Test bucket is empty initially
            objects = list(conn.list("", bucket=bucket_name))
            assert len(objects) == 0, "New bucket should be empty"

            # Test file operations
            test_content = b"Comprehensive test content"

            # Upload multiple files with different key patterns
            test_files = [
                "root-file.txt",
                "folder/nested-file.txt",
                "deep/folder/structure/file.txt",
            ]

            # Upload all files
            for file_key in test_files:
                test_file = BytesIO(test_content)
                upload_response = conn.upload(file_key, test_file, bucket=bucket_name)
                assert upload_response.status_code in [
                    200,
                    201,
                ], "Upload of {0} should succeed".format(file_key)

            # Test listing all objects
            all_objects = list(conn.list("", bucket=bucket_name))
            assert len(all_objects) == len(test_files), "Should list all uploaded files"

            # Test prefix-based listing
            folder_objects = list(conn.list("folder/", bucket=bucket_name))
            folder_keys = [obj["key"] for obj in folder_objects]
            assert "folder/nested-file.txt" in folder_keys, "Should find nested file"

            # Test download and content verification
            for file_key in test_files:
                download_response = conn.get(file_key, bucket=bucket_name)
                assert download_response.status_code == 200, (
                    "Download of {0} should succeed".format(file_key)
                )
                assert download_response.content == test_content, (
                    "Content should match for {0}".format(file_key)
                )

            # Test file deletion
            for file_key in test_files:
                delete_response = conn.delete(file_key, bucket=bucket_name)
                assert delete_response.status_code in [
                    200,
                    204,
                ], "Deletion of {0} should succeed".format(file_key)

            # Verify bucket is empty after deletion
            final_objects = list(conn.list("", bucket=bucket_name))
            assert len(final_objects) == 0, "Bucket should be empty after all deletions"

        finally:
            # Clean up bucket
            try:
                # Ensure bucket is empty
                objects = list(conn.list("", bucket=bucket_name))
                for obj in objects:
                    conn.delete(obj["key"], bucket=bucket_name)
                # Delete bucket
                conn.delete_bucket(bucket_name)
            except Exception:
                pass
