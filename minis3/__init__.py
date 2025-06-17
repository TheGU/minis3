# -*- coding: utf-8 -*-
from __future__ import print_function, division, absolute_import, unicode_literals

from .connection import Connection
from .pool import Pool
from .multipart_upload import MultipartUpload

# Backward comparability with versions prior to 0.1.7
from .connection import Connection as Conn

__title__ = 'minis3'
__version__ = '1.0.0'
__author__ = 'Pattapong Jantarach'
__license__ = 'MIT'
__all__ = ["Connection", "Conn", "Pool", "MultipartUpload"]
