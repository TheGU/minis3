# minis3 - Enhanced S3-Compatible Storage Library for Python

[![PyPI version](https://badge.fury.io/py/minis3.svg)](https://badge.fury.io/py/minis3)
[![Python Versions](https://img.shields.io/pypi/pyversions/minis3.svg)](https://pypi.org/project/minis3/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**minis3** is a modern, maintained fork of the popular `tinys3` library. It provides a simple, Pythonic interface for interacting with Amazon S3 and S3-compatible storage services like MinIO, with enhanced features and better Python 2/3 compatibility.

## Key Features

- **S3-Compatible**: Works with AWS S3, MinIO, and other S3-compatible storage services.
- **Modern Python Support**: Compatible with Python 2.7 and Python 3.6+.
- **Enhanced Security**: AWS Signature Version 4 support by default.
- **Flexible Connection**: Support for custom endpoints, SSL/TLS control, and path-style requests.
- **High-Performance Operations**: Built-in connection pooling for concurrent uploads, downloads, and more.
- **Rich Functionality**: Full range of bucket and object operations, including multipart uploads.
- **Easy to Use**: Simple, `requests`-inspired API.

## Installation

```bash
pip install minis3
```

## Quick Start

Connect to your S3-compatible service by creating a `Connection` object.

```python
import minis3

# Connect to AWS S3
conn = minis3.Connection(
    access_key='YOUR_ACCESS_KEY',
    secret_key='YOUR_SECRET_KEY',
    tls=True  # Use HTTPS (default)
)

# Upload a file
with open('local_file.txt', 'rb') as f:
    conn.upload('remote_key.txt', f, bucket='my-bucket')

# Download a file
response = conn.get('remote_key.txt', bucket='my-bucket')
with open('downloaded_file.txt', 'wb') as f:
    f.write(response.content)
```

## Connecting to Other Services (e.g., MinIO)

To connect to other S3-compatible services like MinIO, you need to specify the `endpoint` and adjust `tls` settings if necessary. For local development, you might also need `path_style=True`.

```python
import minis3

# Connect to a local MinIO server
conn = minis3.Connection(
    access_key='minioadmin',
    secret_key='minioadmin',
    endpoint='localhost:9000',  # Your MinIO endpoint
    tls=False,                  # Use HTTP for local dev
    path_style=True,            # Required for MinIO
    verify=False                # Disable SSL cert verification for self-signed certs
)

# Now you can use the connection as usual
# conn.create_bucket('my-new-bucket')
```

## Connection Parameters

`minis3.Connection` and `minis3.Pool` accept the following parameters:

- `access_key`: Your S3 access key.
- `secret_key`: Your S3 secret key.
- `endpoint`: The S3 endpoint URL. Defaults to `'s3.amazonaws.com'`.
- `default_bucket`: (Optional) A default bucket name to use for all operations.
- `tls`: (Default: `True`) Use HTTPS if `True`, HTTP if `False`.
- `verify`: (Default: `True`) Verify SSL certificates. Set to `False` for services with self-signed certificates.
- `signature_version`: (Default: `'s3v4'`) The AWS signature version. Use `'s3'` for legacy services.
- `path_style`: (Default: `False`) If `True`, use path-style URLs (`endpoint/bucket/key`) instead of virtual host-style (`bucket.endpoint/key`). Required for some S3-compatible services like MinIO.

## Bucket Management

You can create and delete buckets directly.

```python
# Create a new bucket
conn.create_bucket('my-new-bucket')
print("Bucket 'my-new-bucket' created.")

# Delete a bucket
conn.delete_bucket('my-new-bucket')
print("Bucket 'my-new-bucket' deleted.")
```

## Object Operations

`minis3` supports a wide range of object-level operations.

### Uploading Objects

The `upload` method is highly flexible. You can control metadata, caching, and public access.

```python
# Upload a file with custom metadata and caching
with open('profile.jpg', 'rb') as f:
    conn.upload(
        'user_profiles/profile.jpg',
        f,
        bucket='images',
        public=True,  # Make the object publicly readable
        expires='max',  # Cache for 1 year
        content_type='image/jpeg',
        headers={
            'x-amz-storage-class': 'STANDARD_IA',
            'x-amz-meta-user-id': '12345'
        }
    )
```

### Downloading Objects

The `get` method returns a `requests.Response` object.

```python
response = conn.get('user_profiles/profile.jpg', bucket='images')

print(f"Status Code: {response.status_code}")
print(f"Content-Type: {response.headers['Content-Type']}")

# Save the content to a file
with open('downloaded_profile.jpg', 'wb') as f:
    f.write(response.content)
```

### Listing Objects

The `list` method returns an iterator for the objects in a bucket.

```python
# List all objects with a given prefix
for obj in conn.list('user_profiles/', bucket='images'):
    print(
        f"Key: {obj['key']}, "
        f"Size: {obj['size']}, "
        f"Modified: {obj['last_modified']}"
    )
```

### Deleting Objects

```python
conn.delete('user_profiles/profile.jpg', bucket='images')
```

### Copying Objects

Copy objects between buckets or within the same bucket.

```python
conn.copy(
    from_key='user_profiles/profile.jpg',
    from_bucket='images',
    to_key='archived_profiles/profile_old.jpg',
    to_bucket='archive-bucket'
)
```

### Managing Metadata

You can retrieve an object's metadata with `head_object` or update it with `update_metadata`.

```python
# Get object metadata
metadata = conn.head_object('remote_key.txt', bucket='my-bucket')
print(metadata)

# Update metadata for an existing object
conn.update_metadata(
    'remote_key.txt',
    bucket='my-bucket',
    metadata={'x-amz-meta-reviewed': 'true'}
)
```

## High-Performance Operations with `minis3.Pool`

For high-throughput applications, use `minis3.Pool` to perform operations concurrently using a thread pool. The API is the same as `minis3.Connection`, but methods return a `Future` object.

```python
import minis3
import os

# Create a connection pool
pool = minis3.Pool(
    access_key='YOUR_ACCESS_KEY',
    secret_key='YOUR_SECRET_KEY',
    size=20  # Number of worker threads
)

# Upload all files in a directory concurrently
futures = []
for filename in os.listdir('./my_files'):
    filepath = os.path.join('./my_files', filename)
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as f:
            # Note: The file handle must remain open until the future completes
            future = pool.upload(f'uploads/{filename}', f.read(), bucket='my-bucket')
            futures.append(future)

# Wait for all uploads to complete and check results
for future in pool.as_completed(futures):
    response = future.result()
    print(f"Upload completed with status: {response.status_code}")

# The pool can also be used as a context manager
with minis3.Pool(access_key, secret_key, size=10) as pool:
    # ... perform operations
    pass # Pool is automatically closed
```

## Large File Uploads (Multipart)

For files larger than a few megabytes, `minis3` supports multipart uploads. This process is handled by the `MultipartUpload` helper.

```python
from minis3 import MultipartUpload

# The source file
file_path = 'large_video.mp4'

# Initiate the multipart upload
with MultipartUpload(conn, file_path, 'videos/large_video.mp4', bucket='my-bucket') as uploader:
    # The uploader will automatically read the file in chunks and upload them.
    # You can monitor progress if needed.
    print(f"Started multipart upload for {file_path} with ID: {uploader.upload_id}")

print("Multipart upload completed successfully.")
```

## Signature Versions

- **`'s3v4'`** (default): AWS Signature Version 4. Required for all modern AWS regions.
- **`'s3'`**: AWS Signature Version 2. Legacy support for older storage systems.

## Migration from tinys3

`minis3` is designed as a drop-in replacement for `tinys3`. Simply update your import statement:

```python
# Old tinys3 code
# import tinys3
# conn = tinys3.Connection('key', 'secret')

# New minis3 code
import minis3
conn = minis3.Connection('key', 'secret')
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Original [smore-inc/tinys3](https://github.com/smore-inc/tinys3) library by Shlomi Atar
- Inspired by the excellent `requests` library
- AWS S3 API documentation
