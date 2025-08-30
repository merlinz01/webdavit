"""WebDAV exceptions and status codes."""

from typing import Any


class WebDAVError(Exception):
    """Base WebDAV exception."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class WebDAVHttpError(WebDAVError):
    """HTTP error from WebDAV server."""


class WebDAVMultiStatusError(WebDAVHttpError):
    """207 Multi-Status"""

    def __init__(self, message: str, responses: dict[str, Any]):
        super().__init__(message, 207)
        self.responses = responses


class WebDAVForbiddenError(WebDAVHttpError):
    """403 Forbidden"""

    def __init__(self, message: str):
        super().__init__(message, 403)


class WebDAVNotFoundError(WebDAVHttpError):
    """404 Not Found"""

    def __init__(self, message: str):
        super().__init__(message, 404)


class WebDAVMethodNotAllowedError(WebDAVHttpError):
    """405 Method Not Allowed"""

    def __init__(self, message: str):
        super().__init__(message, 405)


class WebDAVConflictError(WebDAVHttpError):
    """409 Conflict"""

    def __init__(self, message: str):
        super().__init__(message, 409)


class WebDAVPreconditionFailedError(WebDAVHttpError):
    """412 Precondition Failed"""

    def __init__(self, message: str):
        super().__init__(message, 412)


class WebDAVRequestURITooLongError(WebDAVHttpError):
    """414 Request-URI Too Long"""

    def __init__(self, message: str):
        super().__init__(message, 414)


class WebDAVUnprocessableEntityError(WebDAVHttpError):
    """422 Unprocessable Entity"""

    def __init__(self, message: str):
        super().__init__(message, 422)


class WebDAVLockedError(WebDAVHttpError):
    """423 Locked"""

    def __init__(self, message: str):
        super().__init__(message, 423)


class WebDAVFailedDependencyError(WebDAVError):
    """424 Failed Dependency"""

    def __init__(self, message: str):
        super().__init__(message, 424)


class WebDAVInsufficientStorageError(WebDAVHttpError):
    """507 Insufficient Storage"""

    def __init__(self, message: str):
        super().__init__(message, 507)


class WebDAVParseError(WebDAVError):
    """XML parsing error."""
