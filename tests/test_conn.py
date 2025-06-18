# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from flexmock import flexmock

from minis3 import Connection
from minis3.auth import S3Auth

TEST_SECRET_KEY = "TEST_SECRET_KEY"
TEST_ACCESS_KEY = "TEST_ACCESS_KEY"
TEST_BUCKET = "bucket"
TEST_DATA = "test test test" * 2


class TestConn(unittest.TestCase):
    def setUp(self):
        self.conn = Connection(
            TEST_ACCESS_KEY, TEST_SECRET_KEY, default_bucket=TEST_BUCKET, tls=True
        )

    def test_creation(self):
        """
        Test the creation of a connection
        """

        self.assertTrue(isinstance(self.conn.auth, S3Auth))
        self.assertEqual(self.conn.default_bucket, TEST_BUCKET)
        self.assertEqual(self.conn.tls, True)
        self.assertEqual(self.conn.endpoint, "s3.amazonaws.com")
