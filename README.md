# minis3 - Enhanced S3-Compatible Storage Library for Python

[![PyPI version](https://badge.fury.io/py/minis3.svg)](https://badge.fury.io/py/minis3)
[![Python Versions](https://img.shields.io/pypi/pyversions/minis3.svg)](https://pypi.org/project/minis3/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**minis3** is a modern, maintained fork of the popular `tinys3` library. It provides a simple, Pythonic interface for interacting with Amazon S3 and S3-compatible storage services like MinIO, with enhanced features and better Python 2/3 compatibility.

## Key Features

- **S3-Compatible**: Works with AWS S3, MinIO, and other S3-compatible storage services
- **Modern Python Support**: Compatible with Python 2.7 and Python 3.6+
- **Enhanced Security**: AWS Signature Version 4 support by default
- **Flexible Connection**: Support for custom endpoints, SSL/TLS control, and certificate verification
- **Async Operations**: Built-in connection pooling for high-performance uploads
- **Rich Functionality**: Upload, download, copy, delete, list, and metadata operations
- **Easy to Use**: Simple, requests-inspired API

## Installation

```bash
pip install minis3
```

## Quick Start

### Basic Usage with AWS S3

```python
import minis3

# Connect to AWS S3 (default)
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

### Connecting to MinIO or Other S3-Compatible Services

```python
import minis3

# Connect to MinIO server
conn = minis3.Connection(
    access_key='minioadmin',
    secret_key='minioadmin',
    endpoint='localhost:9000',  # MinIO default
    tls=False,  # Use HTTP for local development
    verify=False  # Skip SSL verification for self-signed certificates
)

# Upload a file to MinIO
with open('my_file.pdf', 'rb') as f:
    conn.upload('documents/my_file.pdf', f, bucket='my-bucket')
```

### Using Connection Pools for High Performance

```python
import minis3

# Create a connection pool for concurrent operations
pool = minis3.Pool(
    access_key='YOUR_ACCESS_KEY',
    secret_key='YOUR_SECRET_KEY',
    size=10  # Number of worker threads
)

# Upload multiple files concurrently
futures = []
for i in range(10):
    with open(f'file_{i}.txt', 'rb') as f:
        future = pool.upload(f'uploads/file_{i}.txt', f, bucket='my-bucket')
        futures.append(future)

# Wait for all uploads to complete
for future in futures:
    response = future.result()
    print(f"Upload completed: {response.status_code}")
```

## Advanced Usage

### Custom Headers and Metadata

```python
# Upload with custom headers
conn.upload(
    'my_file.jpg',
    file_obj,
    bucket='images',
    headers={
        'x-amz-storage-class': 'REDUCED_REDUNDANCY',
        'x-amz-meta-author': 'John Doe'
    },
    content_type='image/jpeg',
    public=False  # Make file publicly accessible
)
```

### Working with Different AWS Regions

```python
# Connect to specific AWS region
conn = minis3.Connection(
    access_key='YOUR_ACCESS_KEY',
    secret_key='YOUR_SECRET_KEY',
    endpoint='s3.eu-west-1.amazonaws.com',  # Europe region
    tls=True
)
```

### List Objects in a Bucket

```python
# List all objects with a prefix
for obj in conn.list('photos/', bucket='my-bucket'):
    print(f"Key: {obj['key']}, Size: {obj['size']}, Modified: {obj['last_modified']}")
```

### Copy and Delete Operations

```python
# Copy an object
conn.copy(
    from_key='old_location/file.txt',
    from_bucket='source-bucket',
    to_key='new_location/file.txt',
    to_bucket='destination-bucket'
)

# Delete an object
conn.delete('unwanted_file.txt', bucket='my-bucket')
```

## Configuration Options

### Connection Parameters

- `access_key`: Your S3 access key
- `secret_key`: Your S3 secret key  
- `endpoint`: S3 endpoint URL (default: 's3.amazonaws.com')
- `tls`: Use HTTPS if True, HTTP if False (default: True)
- `verify`: Verify SSL certificates (default: True)
- `signature_version`: AWS signature version - 's3v4' (default) or 's3' for legacy
- `default_bucket`: Default bucket name for operations

### Signature Versions

- **'s3v4'** (default): AWS Signature Version 4 - required for newer AWS regions
- **'s3'**: AWS Signature Version 2 - legacy support for older systems

## Requirements

- Python 2.7 or Python 3.6+
- requests library

## Migration from tinys3

Simply replace `import tinys3` with `import minis3` - the API is fully compatible:

```python
# Old tinys3 code
import tinys3
conn = tinys3.Connection('key', 'secret')

# New minis3 code  
import minis3
conn = minis3.Connection('key', 'secret')
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Original `tinys3` library by Shlomi Atar
- Inspired by the excellent `requests` library
- AWS S3 API documentation

---

**Note**: This is a community-maintained fork of the original `tinys3` project. The original project can be found at [smore-inc/tinys3](https://github.com/smore-inc/tinys3).

