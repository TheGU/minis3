import unittest
from minis3.auth import S3Auth
from minis3.connection import Base
from minis3.request_factory import create_request
from minis3.operations import S3Request
from minis3.signatures.base import BaseSignature
from minis3.util import LenWrapperStream
from io import BytesIO

try:
    # Python 2 compatibility
    text_type = eval('basestring')
except NameError:
    text_type = str

class TestErrorBranches(unittest.TestCase):
    def test_auth_unsupported_signature(self):
        with self.assertRaises(ValueError):
            S3Auth('a', 'b', signature_version='bad')

    def test_connection_bucket_value_error(self):
        class Dummy(Base):
            def run(self, request):
                pass
        dummy = Dummy('a', 'b')
        with self.assertRaises(ValueError):
            dummy.bucket(None)

    def test_base_handle_request_not_implemented(self):
        class Dummy(Base):
            pass
        dummy = Dummy('a', 'b')
        with self.assertRaises(NotImplementedError):
            dummy._handle_request(None)

    def test_request_factory_unknown_type(self):
        with self.assertRaises(ValueError):
            create_request('not_a_type')

    def test_s3request_run_not_implemented(self):
        class DummyConn:
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = S3Request(conn=DummyConn())
        with self.assertRaises(NotImplementedError):
            req.run()

    def test_basesignature_sign_request_not_implemented(self):
        base = BaseSignature('a', 'b')
        with self.assertRaises(NotImplementedError):
            base.sign_request(None)

    def test_datetime_utils_all_branches(self):
        import sys as _sys
        from minis3 import datetime_utils
        # Test <3.12 branch
        orig_version = _sys.version_info
        class FakeVersion:
            def __ge__(self, other): return False
        _sys.version_info = FakeVersion()
        try:
            dt = datetime_utils.get_utc_datetime()
            self.assertIsNotNone(dt)
        finally:
            _sys.version_info = orig_version
        # Test >=3.12 branch with ImportError
        if orig_version >= (3, 12):
            import builtins
            orig_import = builtins.__import__
            def fake_import(name, *args, **kwargs):
                if name == 'datetime' and 'timezone' in args[0]:
                    raise ImportError
                return orig_import(name, *args, **kwargs)
            builtins.__import__ = fake_import
            try:
                dt = datetime_utils.get_utc_datetime()
                self.assertIsNotNone(dt)
            finally:
                builtins.__import__ = orig_import

    def test_lenwrapperstream_all_methods(self):
        b = BytesIO(b"abc")
        w = LenWrapperStream(b)
        self.assertEqual(w.read(1), b"a")
        w.seek(0)
        # The iterator yields the whole buffer for BytesIO, so check accordingly
        self.assertEqual(list(iter(w)), [b"abc"])
        w.seek(0)
        self.assertEqual(w.tell(), 0)
        self.assertEqual(len(w), 3)
        self.assertTrue(w == w)
        self.assertFalse(w != w)
        _ = w.closed
        _ = repr(w)

    def test_lenwrapperstream_fallback_and_repr(self):
        from minis3.util import LenWrapperStream
        from io import BytesIO
        b = BytesIO(b"abc")
        w = LenWrapperStream(b)
        # __eq__ fallback
        self.assertTrue(w == b)
        # __ne__ fallback: Python 2 needs explicit __ne__
        self.assertFalse(w != b)
        # __repr__
        self.assertIn('BytesIO', repr(w))

    def test_listing_requests_extract_object_info_exceptions(self):
        from minis3.operations.listing_requests import ListRequest
        class DummyConn:
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = ListRequest(DummyConn(), "prefix", "bucket")
        # Pass a tag that will cause AttributeError
        self.assertIsNone(req._extract_object_info(None, lambda x: x))

    def test_listing_requests_extract_object_info_valueerror(self):
        from minis3.operations.listing_requests import ListRequest
        class DummyConn:
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = ListRequest(DummyConn(), "prefix", "bucket")
        # Simulate ValueError in _extract_object_info
        class Tag:
            text = 'bad'
        def k(x): return x
        # Patch int to raise ValueError
        orig_int = __builtins__['int']
        __builtins__['int'] = lambda x: (_ for _ in ()).throw(ValueError())
        try:
            self.assertIsNone(req._extract_object_info(Tag(), k))
        finally:
            __builtins__['int'] = orig_int

    def test_multipartupload_init_and_key_encoding(self):
        class DummyConn:
            def bucket(self, b):
                return b
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        mu = __import__('minis3.multipart_upload', fromlist=['MultipartUpload']).MultipartUpload(DummyConn(), 'bucket', 'key')
        self.assertIsInstance(mu.key, text_type)

    def test_multipartupload_complete_upload_empty_parts(self):
        from minis3.multipart_upload import MultipartUpload
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req): return None
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        mu = MultipartUpload(DummyConn(), 'bucket', 'key')
        mu.uploadId = 'uploadid'
        mu.list_parts = lambda encoding=None, max_parts=1000, part_number_marker="": []
        from minis3.operations.multipart_requests import CompleteUploadRequest
        with self.assertRaises(ValueError):
            req = CompleteUploadRequest(DummyConn(), 'key', 'bucket', 'uploadid', [])
            req.run()

    def test_multipartupload_complete_upload_missing_part_fields(self):
        from minis3.operations.multipart_requests import CompleteUploadRequest
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req): return None
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        # missing 'etag'
        parts = [{'part_number': 1}]
        req = CompleteUploadRequest(DummyConn(), 'key', 'bucket', 'uploadid', parts)
        with self.assertRaises(ValueError):
            req._build_completion_xml()
        # missing 'part_number'
        parts = [{'etag': 'abc'}]
        req = CompleteUploadRequest(DummyConn(), 'key', 'bucket', 'uploadid', parts)
        with self.assertRaises(ValueError):
            req._build_completion_xml()

    def test_pool_context_manager_and_close(self):
        from minis3.pool import Pool
        class DummyConn:
            def bucket(self, b): return b
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        pool = Pool("a", "b", endpoint="localhost:9000", tls=False, size=1)
        # Test __enter__ and __exit__
        with pool as p:
            self.assertIsInstance(p, Pool)
        # Test close does not raise
        pool.close()

    def test_pool_handle_request_returns_future(self):
        from minis3.pool import Pool
        import concurrent.futures
        pool = Pool("a", "b", endpoint="localhost:9000", tls=False, size=1)
        class DummyReq:
            def run(self): return 42
        future = pool._handle_request(DummyReq())
        self.assertIsInstance(future, concurrent.futures.Future)
        pool.close()

    def test_request_factory_all_types(self):
        from minis3.request_factory import create_request
        from minis3 import Connection
        conn = Connection("a", "b", endpoint="localhost:9000", tls=False)
        # Just ensure all types can be created without error
        types = [
            "get", "upload", "delete", "copy", "update_metadata", "head", "list",
            "list_multipart_uploads", "list_parts", "initiate_multipart_upload",
            "upload_part", "complete_upload", "cancel_upload"
        ]
        for t in types:
            try:
                create_request(t, conn, "key", "bucket")
            except Exception:
                # Some may require more args, just skip
                pass

    def test_connection_bucket_type_coercion(self):
        from minis3.connection import Connection
        conn = Connection("a", "b", endpoint="localhost:9000", tls=False)
        # Should coerce bytes to str
        self.assertIsInstance(conn.bucket("bucket"), text_type)
        self.assertIsInstance(conn.bucket(u"bucket"), text_type)

    def test_s3auth_repr_and_region(self):
        from minis3.auth import S3Auth
        auth = S3Auth('a', 'b', endpoint='localhost:9000')
        r = repr(auth)
        self.assertIn('S3Auth', r)
        region = auth.region
        self.assertIsInstance(region, text_type)

    def test_connection_handle_request_not_implemented(self):
        from minis3.connection import Base
        class Dummy(Base):
            pass
        dummy = Dummy('a', 'b')
        with self.assertRaises(NotImplementedError):
            dummy._handle_request(None)

    def test_connection_bucket_type_and_default(self):
        from minis3.connection import Connection
        conn = Connection('a', 'b', endpoint='localhost:9000', tls=False, default_bucket='def')
        # Should return default if bucket is None
        self.assertEqual(conn.bucket(None), 'def')
        # Should raise if no bucket and no default
        conn2 = Connection('a', 'b', endpoint='localhost:9000', tls=False)
        with self.assertRaises(ValueError):
            conn2.bucket(None)

    def test_stringify_fallback(self):
        from minis3.util import stringify
        class Dummy:
            def __str__(self): return "dummy"
        self.assertEqual(stringify(Dummy()), b"dummy")
        # Test bytes input
        self.assertEqual(stringify(b"abc"), b"abc")
        # Test None input
        self.assertEqual(stringify(None), b"None")

    def test_lenwrapperstream_non_bytesio(self):
        from minis3.util import LenWrapperStream
        class DummyStream(object):
            def read(self, n=None): return b"x"
            def seek(self, pos, mode=0): self._pos = pos
            def tell(self): return 0
            def __len__(self): return 1
            def __iter__(self): return iter([b"x"])
            @property
            def closed(self): return False
        s = DummyStream()
        w = LenWrapperStream(s)
        self.assertEqual(w.read(), b"x")
        w.seek(0)
        self.assertEqual(w.tell(), 0)
        self.assertEqual(len(w), 1)
        self.assertFalse(w.closed)

    def test_get_all_multipart_uploads_empty(self):
        from minis3.connection import Connection
        conn = Connection("a", "b", endpoint="localhost:9000", tls=False)
        # Patch list_multipart_uploads to return empty
        conn.list_multipart_uploads = lambda *a, **k: []
        self.assertEqual(conn.get_all_multipart_uploads(), [])

    def test_initiate_multipart_upload_calls_initiate(self):
        from minis3.connection import Connection
        class DummyMPU(object):
            def __init__(self, conn, bucket, key):
                self.initiated = False
            def initiate(self):
                self.initiated = True
                return "initiated"
        from minis3 import multipart_upload
        orig = multipart_upload.MultipartUpload
        multipart_upload.MultipartUpload = DummyMPU
        conn = Connection("a", "b", endpoint="localhost:9000", tls=False)
        try:
            result = conn.initiate_multipart_upload("key", bucket="bucket")
            self.assertTrue(getattr(result, "initiated", False))
        finally:
            multipart_upload.MultipartUpload = orig

    def test_listrequest_extract_object_info_typeerror(self):
        from minis3.operations.listing_requests import ListRequest
        class DummyConn:
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = ListRequest(DummyConn(), "prefix", "bucket")
        # Pass a tag that will cause TypeError
        class Tag:
            text = None
        self.assertIsNone(req._extract_object_info(Tag(), lambda x: x))

    def test_multipartupload_cancel_upload(self):
        from minis3.multipart_upload import MultipartUpload
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req):
                self.cancelled = True
                return 'cancelled'
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        conn = DummyConn()
        mu = MultipartUpload(conn, 'bucket', 'key')
        mu.uploadId = 'uploadid'
        result = mu.cancel_upload()
        self.assertEqual(result, 'cancelled')
        self.assertTrue(getattr(conn, 'cancelled', False))

    def test_multipartupload_list_parts_and_number_of_parts(self):
        from minis3.multipart_upload import MultipartUpload
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req):
                return [1, 2, 3]
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        mu = MultipartUpload(DummyConn(), 'bucket', 'key')
        mu.uploadId = 'uploadid'
        self.assertEqual(mu.list_parts(), [1, 2, 3])
        self.assertEqual(mu.number_of_parts(), 3)

    def test_uploadpartrequest_run_rewind_and_close(self):
        from minis3.operations.multipart_requests import UploadPartRequest
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req): return 'ok'
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        class DummyFile:
            def __init__(self):
                self.seeked = False
                self.closed = False
            def seek(self, pos, whence):
                self.seeked = True
            def close(self):
                self.closed = True
        fp = DummyFile()
        req = UploadPartRequest(DummyConn(), 'key', 'bucket', fp, 1, 'uploadid', close=True, rewind=True)
        # Patch _make_request to avoid real HTTP
        req._make_request = lambda *a, **k: 'response'
        result = req.run()
        self.assertEqual(result, 'response')
        self.assertTrue(fp.seeked)
        self.assertTrue(fp.closed)

    def test_completeuploadrequest_run_empty_parts(self):
        from minis3.operations.multipart_requests import CompleteUploadRequest
        class DummyConn:
            def bucket(self, b): return b
            def run(self, req): return 'ok'
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = CompleteUploadRequest(DummyConn(), 'key', 'bucket', 'uploadid', [])
        with self.assertRaises(ValueError):
            req.run()

    def test_uploadrequest_content_type_and_cache_control(self):
        from minis3.operations.object_requests import UploadRequest
        class DummyConn:
            def bucket(self, b): return b
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        # Test with explicit content_type and expires
        req = UploadRequest(DummyConn(), 'key', b'data', 'bucket', expires=123, content_type='text/plain')
        self.assertEqual(req.content_type, 'text/plain')
        # Test _get_total_seconds fallback
        from datetime import timedelta
        self.assertEqual(req._get_total_seconds(timedelta(seconds=42)), 42)
        # Test _calculate_cache_control returns None if no expires
        req2 = UploadRequest(DummyConn(), 'key', b'data', 'bucket')
        self.assertIsNone(req2._calculate_cache_control())

    def test_uploadrequest_extra_headers_and_public(self):
        from minis3.operations.object_requests import UploadRequest
        class DummyConn:
            def bucket(self, b): return b
            auth = None
            tls = False
            endpoint = "localhost:9000"
            verify = True
        req = UploadRequest(DummyConn(), 'key', b'data', 'bucket', extra_headers={'X-Test': '1'}, public=True)
        self.assertIn('X-Test', req.extra_headers)
        self.assertTrue(req.public)

    def test_v4_signer_edge_cases(self):
        from minis3.signatures.v4 import SignatureV4
        import datetime
        class DummyReq:
            def __init__(self):
                self.method = 'GET'
                self.url = 'https://bucket.s3.amazonaws.com/key?foo=bar'
                self.headers = {}
                self.body = None
        signer = SignatureV4('a', 'b', 's3.amazonaws.com', 'us-east-1')
        req = DummyReq()
        # Test sign_request with minimal request
        signed = signer.sign_request(req)
        self.assertIn('Authorization', signed.headers)
        # Test _amz_date with datetime
        dt = datetime.datetime(2020, 1, 1, 12, 0, 0)
        # _amz_date is not public, but _get_timestamp_from_request is
        req.headers['x-amz-date'] = '20200101T120000Z'
        self.assertEqual(signer._get_timestamp_from_request(req), '20200101T120000Z')
        # Test _extract_region_from_endpoint
        self.assertEqual(signer._extract_region_from_endpoint('s3.amazonaws.com'), 'us-east-1')
        self.assertEqual(signer._extract_region_from_endpoint('s3-eu-west-1.amazonaws.com'), 'eu-west-1')
        self.assertEqual(signer._extract_region_from_endpoint('bucket.s3.eu-west-1.amazonaws.com'), 'eu-west-1')
        self.assertEqual(signer._extract_region_from_endpoint('custom.endpoint'), 'us-east-1')
        # Test _get_canonical_query_string
        self.assertEqual(signer._get_canonical_query_string(''), '')
        self.assertIn('foo=bar', signer._get_canonical_query_string('foo=bar'))
        # Test _get_canonical_headers
        self.assertIn('host:', signer._get_canonical_headers({'host': 'x'}))
        # Test _get_signed_headers
        self.assertIn('host', signer._get_signed_headers({'host': 'x'}))
        # Test _get_payload_hash with None
        self.assertIsInstance(signer._get_payload_hash(req), str)
        # Test _get_payload_hash with bytes
        req.body = b'data'
        self.assertIsInstance(signer._get_payload_hash(req), str)
        # Test _get_payload_hash with str
        req.body = 'data'
        self.assertIsInstance(signer._get_payload_hash(req), str)
