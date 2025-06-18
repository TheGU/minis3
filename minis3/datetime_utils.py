# -*- coding: utf-8 -*-
"""
minis3.datetime_utils
~~~~~~~~~~~~~~~~~~~~

Datetime utilities for Python 2/3 compatibility.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import sys
from datetime import datetime


def get_utc_datetime():
    """
    Get current UTC datetime in a Python 2/3 compatible way.

    This function handles the deprecation of datetime.utcnow() in Python 3.12+
    while maintaining compatibility with Python 2.7 and older Python 3 versions.

    Returns:
        datetime: Current UTC datetime
    """
    if sys.version_info >= (3, 12):
        # Python 3.12+ - use the new timezone-aware approach
        try:
            from datetime import timezone

            return datetime.now(timezone.utc)
        except ImportError:
            # Fallback for systems without timezone support
            return datetime.utcnow()
    else:
        # Python < 3.12 and Python 2.7
        return datetime.utcnow()
