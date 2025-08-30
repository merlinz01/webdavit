"""
WebDAVit - A simple async WebDAV client for Python
"""

from .client import WebDAVClient, WebDAVResource
from .exceptions import (
    WebDAVError,
    WebDAVFailedDependencyError,
    WebDAVHttpError,
    WebDAVLockedError,
    WebDAVMultiStatusError,
    WebDAVParseError,
    WebDAVConflictError,
    WebDAVUnprocessableEntityError,
    WebDAVForbiddenError,
    WebDAVNotFoundError,
    WebDAVPreconditionFailedError,
    WebDAVRequestURITooLongError,
    WebDAVInsufficientStorageError,
    WebDAVMethodNotAllowedError,
)

__version__ = "0.1.0"
__all__ = [
    "WebDAVClient",
    "WebDAVError",
    "WebDAVFailedDependencyError",
    "WebDAVHttpError",
    "WebDAVLockedError",
    "WebDAVMultiStatusError",
    "WebDAVParseError",
    "WebDAVResource",
    "WebDAVConflictError",
    "WebDAVUnprocessableEntityError",
    "WebDAVForbiddenError",
    "WebDAVNotFoundError",
    "WebDAVPreconditionFailedError",
    "WebDAVMethodNotAllowedError",
    "WebDAVRequestURITooLongError",
    "WebDAVInsufficientStorageError",
]
