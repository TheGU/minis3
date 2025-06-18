import unittest
from minis3.request_factory import S3Request, ListRequest, UploadRequest, GetRequest, DeleteRequest, UpdateMetadataRequest, CopyRequest
from minis3 import Connection
from flexmock import flexmock

class DummyConn(Connection):
    def __init__(self):
        super(DummyConn, self).__init__("key", "secret", endpoint="localhost:9000", tls=False)
        self.auth = None

class TestRequestFactory(unittest.TestCase):
    def setUp(self):
        self.conn = DummyConn()

    def test_s3request_bucket_url(self):
        req = S3Request(self.conn)
        url = req.bucket_url("key", "bucket")
        # Accept both AWS and custom endpoint formats
        self.assertTrue(
            "bucket.s3.amazonaws.com" in url or "localhost:9000" in url
        )

    def test_list_request(self):
        req = ListRequest(self.conn, "prefix", "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(get=lambda *a, **k: flexmock(raise_for_status=lambda: None, content=b"<ListBucketResult></ListBucketResult>")))
        list(req.run())

    def test_upload_request(self):
        req = UploadRequest(self.conn, "key", b"data", "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(put=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_get_request(self):
        req = GetRequest(self.conn, "key", "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(get=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_delete_request(self):
        req = DeleteRequest(self.conn, "key", "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(delete=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_update_metadata_request(self):
        req = UpdateMetadataRequest(self.conn, "key", "bucket", {"meta": "val"}, True)
        flexmock(req).should_receive("adapter").and_return(flexmock(put=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_copy_request(self):
        req = CopyRequest(self.conn, "from_key", "from_bucket", "to_key", "to_bucket", None, False)
        flexmock(req).should_receive("adapter").and_return(flexmock(put=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_create_bucket_request(self):
        from minis3.operations.bucket_requests import CreateBucketRequest
        req = CreateBucketRequest(self.conn, "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(put=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()

    def test_delete_bucket_request(self):
        from minis3.operations.bucket_requests import DeleteBucketRequest
        req = DeleteBucketRequest(self.conn, "bucket")
        flexmock(req).should_receive("adapter").and_return(flexmock(delete=lambda *a, **k: flexmock(raise_for_status=lambda: None)))
        req.run()
