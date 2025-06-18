# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

# Backward comparability with versions prior to 0.1.7
from .connection import Connection
from .connection import Connection as Conn
from .multipart_upload import MultipartUpload
from .pool import Pool

__title__ = "minis3"
__version__ = "1.0.0"
__author__ = "Pattapong Jantarach"
__license__ = "MIT"
__all__ = ["Connection", "Conn", "Pool", "MultipartUpload"]
