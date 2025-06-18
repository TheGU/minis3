# -*- coding: utf-8 -*-
"""
minis3.signatures
~~~~~~~~~~~~~~~~~

AWS Signature implementations for different versions.
Supports both AWS Signature Version 2 and Version 4.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from .base import BaseSignature
from .v2 import SignatureV2
from .v4 import SignatureV4

__all__ = ["BaseSignature", "SignatureV2", "SignatureV4"]
