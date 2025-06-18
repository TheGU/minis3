import unittest
from flexmock import flexmock
from minis3.multipart_upload import MultipartUpload
from minis3 import Connection

class DummyConn(Connection):
    def __init__(self):
        super(DummyConn, self).__init__("key", "secret", endpoint="localhost:9000", tls=False)
        self.uploaded_parts = []
        self.completed = False
        self.aborted = False
    def upload_part(self, *args, **kwargs):
        self.uploaded_parts.append((args, kwargs))
        return {"ETag": "dummy-etag"}
    def complete_multipart_upload(self, *args, **kwargs):
        self.completed = True
        return True
    def abort_multipart_upload(self, *args, **kwargs):
        self.aborted = True
        return True

class TestMultipartUploadUnit(unittest.TestCase):
    def test_number_of_parts(self):
        conn = DummyConn()
        mu = MultipartUpload(conn, "bucket", "key")
        flexmock(mu).should_receive("list_parts").and_return([1, 2, 3])
        self.assertEqual(mu.number_of_parts(), 3)

    def test_upload_part(self):
        conn = DummyConn()
        mu = MultipartUpload(conn, "bucket", "key")
        # Mock conn.run to simulate upload
        flexmock(conn).should_receive("run").and_return({"ETag": "dummy-etag"})
        result = mu.upload_part_from_file(fp=b"data", part_num=1)
        self.assertEqual(result["ETag"], "dummy-etag")

    def test_repr(self):
        conn = DummyConn()
        mu = MultipartUpload(conn, "bucket", "key")
        self.assertIn("MultipartUpload", repr(mu))
