"""
WebDAVit client implementation.
"""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, BinaryIO
from urllib.parse import urlparse

import aiohttp

from .exceptions import (
    WebDAVConflictError,
    WebDAVError,
    WebDAVFailedDependencyError,
    WebDAVForbiddenError,
    WebDAVHttpError,
    WebDAVInsufficientStorageError,
    WebDAVLockedError,
    WebDAVMethodNotAllowedError,
    WebDAVMultiStatusError,
    WebDAVNotFoundError,
)
from .xml_utils import (
    DAV_NAMESPACE,
    XMLNS_DAV,
    create_lock_xml,
    create_propfind_request,
    get_error_details,
    parse_lock_response,
    parse_move_copy_multistatus_response,
    parse_propfind_result,
)


@dataclass
class WebDAVResource:
    """
    Dataclass representing a WebDAV resource.
    """

    href: str
    """The server URL path of the resource."""
    name: str
    """The name of the resource (last part of the path)."""
    is_dir: bool
    """True if the resource is a directory (collection)."""
    size: int | None
    """Size of the resource in bytes, or None if not applicable."""
    content_type: str | None
    """MIME content type of the resource, or None if not available."""
    created: str | None
    """Creation timestamp as a string, or None if not available."""
    modified: str | None
    """Last modified timestamp as a string, or None if not available."""


@dataclass
class WebDAVLockInfo:
    """
    Dataclass representing WebDAV lock information.
    """

    token: str
    """The lock token string."""
    root: str
    """The path of the locked resource."""
    scope: str
    """The lock scope, typically 'exclusive' or 'shared'."""
    type: str
    """The lock type, typically 'write'."""
    owner: str | None
    """The owner string provided when the lock was created, or None."""
    timeout: str | None
    """The timeout for the lock, or None."""
    depth: str
    """The depth of the lock, typically '0' or 'infinity'."""


class WebDAVClient:
    """
    Async WebDAV client.
    """

    def __init__(
        self,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        timeout: int = 30,
        **kwargs: Any,
    ):
        self.base_url: str = base_url.rstrip("/")
        if not self.base_url.startswith("http://") and not self.base_url.startswith(
            "https://"
        ):
            raise ValueError("base_url must start with http:// or https://")
        self.base_path = urlparse(self.base_url).path
        self._username: str | None = username
        self._password: str | None = password
        self._timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=timeout)
        self._session = None
        self._session_kwargs = kwargs

    async def __aenter__(self) -> "WebDAVClient":
        """
        Initialize underlying HTTP session.
        """
        if self._session is not None:
            raise RuntimeError("Client session already initialized.")
        auth = None
        if self._username and self._password:
            auth = aiohttp.BasicAuth(self._username, self._password)

        self._session = aiohttp.ClientSession(
            auth=auth, timeout=self._timeout, **self._session_kwargs
        )
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Close underlying HTTP session.
        """
        if self._session is None:
            raise RuntimeError("Client session not initialized.")
        await self._session.__aexit__(exc_type, exc_val, exc_tb)
        self._session = None

    async def _request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: str | bytes | BinaryIO | None = None,
        expected_statuses: list[int] = [200],
    ) -> tuple[int, bytes]:
        """
        Make a HTTP request. Returns status code and response content.

        Arguments:
            method: HTTP method (e.g. 'GET', 'PUT', etc.)
            path: URL path relative to base_url
            headers: Optional HTTP headers
            data: Optional request body (str, bytes, or file-like object)
            expected_statuses: List of expected HTTP status codes

        Returns:
            Tuple of (status code, response content as bytes)

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        async with self._request_context(
            method, path, headers, data, expected_statuses
        ) as response:
            return response.status, await response.read()

    @asynccontextmanager
    async def _request_context(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: str | bytes | BinaryIO | None = None,
        expected_statuses: list[int] = [200],
    ) -> AsyncGenerator[aiohttp.ClientResponse, None]:
        """
        Make a HTTP request and yield the response context.

        Arguments:
            method: HTTP method (e.g. 'GET', 'PUT', etc.)
            path: URL path relative to base_url
            headers: Optional HTTP headers
            data: Optional request body (str, bytes, or file-like object)
            expected_statuses: List of expected HTTP status codes

        Yields:
            aiohttp.ClientResponse object

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        if self._session is None:
            raise RuntimeError(
                "Client session not initialized. Use async context manager or call __aenter__."
            )

        url = self.base_url + "/" + path.lstrip("/")

        async with self._session.request(
            method,
            url,
            headers=headers,
            data=data,
            allow_redirects=False,
        ) as response:
            await self._check_common_errors(response, expected_statuses)
            yield response

    async def _check_common_errors(
        self,
        response: aiohttp.ClientResponse,
        expected_statuses: list[int],
    ) -> None:
        """
        Check for common WebDAV errors in the response.

        Arguments:
            response: The aiohttp.ClientResponse object.
            expected_statuses: List of expected HTTP status codes.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
        """
        if response.status in expected_statuses:
            return
        elif response.status == 400:
            raise WebDAVHttpError(
                "Bad request. " + get_error_details(await response.read()),
                response.status,
            )
        elif response.status == 403:
            raise WebDAVForbiddenError(
                "Access forbidden. " + get_error_details(await response.read())
            )
        elif response.status == 404:
            raise WebDAVNotFoundError(
                "Resource not found. " + get_error_details(await response.read())
            )
        elif response.status == 405:
            raise WebDAVMethodNotAllowedError(
                "Method not allowed. " + get_error_details(await response.read())
            )
        elif response.status == 409:
            raise WebDAVConflictError(
                "Conflict. " + get_error_details(await response.read())
            )
        elif response.status == 423:
            raise WebDAVLockedError(
                "Resource is locked. " + get_error_details(await response.read())
            )
        elif response.status == 424:
            raise WebDAVFailedDependencyError(
                "Failed dependency. " + get_error_details(await response.read())
            )
        elif response.status >= 300 and response.status < 400:
            raise WebDAVHttpError(
                f"Redirects are not supported. Got {response.status} Location: {response.headers.get('Location')}",
                response.status,
            )
        else:
            raise WebDAVHttpError(
                f"Got unexpected status code {response.status}: {get_error_details(await response.read())}",
                response.status,
            )

    async def exists(self, path: str) -> bool:
        """
        Check if a resource exists.

        Arguments:
            path: The path of the resource to check.

        Returns:
            True if the resource exists, False otherwise.

        Raises:
            aiohttp.ClientError: Network-related errors.
        """
        status, response_data = await self._request(
            "HEAD",
            path,
            expected_statuses=[200, 207, 404],
        )
        return status != 404

    async def get(self, path: str) -> WebDAVResource:
        """
        Get resource metadata.

        Arguments:
            None

        Returns:
            WebDAVResource object with metadata.

        Raises:
            WebDAVNotFoundError: The resource does not exist.
            WebDAVHttpError: Other HTTP errors.
            aiohttp.ClientError: Network-related errors.
        """
        if not path.startswith("/"):
            path = "/" + path
        href = self.base_path + path
        properties = [
            f"{{{DAV_NAMESPACE}}}resourcetype",
            f"{{{DAV_NAMESPACE}}}getcontentlength",
            f"{{{DAV_NAMESPACE}}}creationdate",
            f"{{{DAV_NAMESPACE}}}getlastmodified",
            f"{{{DAV_NAMESPACE}}}displayname",
            f"{{{DAV_NAMESPACE}}}getcontenttype",
        ]
        propfind_xml = create_propfind_request(properties)
        headers = {
            "Content-Type": "application/xml; charset=utf-8",
            "Depth": "0",
        }
        status, response_data = await self._request(
            "PROPFIND",
            path,
            headers=headers,
            data=propfind_xml,
            expected_statuses=[207],
        )
        results = parse_propfind_result(response_data)
        if href not in results:
            raise WebDAVNotFoundError(f"Resource not included in result: {path}")

        return self._extract_propfind_resource(href, results[href])

    async def list(self, path: str) -> list[WebDAVResource]:
        """
        List directory contents.

        Arguments:
            path: The directory path to list.

        Returns:
            List of WebDAVResource objects representing the contents.

        Raises:
            WebDAVNotFoundError: The directory does not exist.
            WebDAVHttpError: Other HTTP errors.
            aiohttp.ClientError: Network-related errors.
        """
        if not path.endswith("/"):
            path += "/"
        properties = [
            f"{{{DAV_NAMESPACE}}}resourcetype",
            f"{{{DAV_NAMESPACE}}}getcontentlength",
            f"{{{DAV_NAMESPACE}}}creationdate",
            f"{{{DAV_NAMESPACE}}}getlastmodified",
            f"{{{DAV_NAMESPACE}}}displayname",
            f"{{{DAV_NAMESPACE}}}getcontenttype",
        ]
        propfind_xml = create_propfind_request(properties)
        headers = {
            "Content-Type": "application/xml; charset=utf-8",
            "Depth": "1",
        }
        status, response_data = await self._request(
            "PROPFIND",
            path,
            headers=headers,
            data=propfind_xml,
            expected_statuses=[207],
        )
        results = parse_propfind_result(response_data)
        items = []
        for href, props in results.items():
            if href == self.base_path + "/" + path:  # Skip the directory itself
                continue
            items.append(self._extract_propfind_resource(href, props))
        return items

    def _extract_propfind_resource(
        self, href: str, props: dict[str, Any]
    ) -> WebDAVResource:
        """
        Extract WebDAVResource from PROPFIND properties.

        Arguments:
            href: The href of the resource.
            props: The properties dictionary from PROPFIND response.

        Returns:
            WebDAVResource object.

        Raises:
            None
        """
        is_dir = False
        res_type = props.get(f"{{{DAV_NAMESPACE}}}resourcetype", {}).get("value")
        if res_type is not None and not isinstance(res_type, str):
            collection_el = res_type.find("D:collection", namespaces=XMLNS_DAV)
            if collection_el is not None:
                is_dir = True
        size_str = props.get(f"{{{DAV_NAMESPACE}}}getcontentlength", {}).get("value")
        size = None
        if size_str:
            try:
                size = int(size_str)
            except ValueError:
                pass
        content_type = props.get(f"{{{DAV_NAMESPACE}}}getcontenttype", {}).get("value")
        created = props.get(f"{{{DAV_NAMESPACE}}}creationdate", {}).get("value")
        modified = props.get(f"{{{DAV_NAMESPACE}}}getlastmodified", {}).get("value")
        name = props.get(f"{{{DAV_NAMESPACE}}}displayname", {}).get("value")
        if not name:
            name = href.rstrip("/").split("/")[-1]
        return WebDAVResource(
            href=href,
            name=name,
            is_dir=is_dir,
            size=size,
            content_type=content_type,
            created=created,
            modified=modified,
        )

    async def mkdir(self, path: str, exist_ok: bool = False) -> None:
        """
        Create a directory at the specified path.

        Arguments:
            path: The directory path to create. May end with a slash (/).

        Returns:
            None

        Raises:
            WebDAVHttpError: The directory cannot be created.
        """
        if not path.endswith("/"):
            path += "/"
        status, response_data = await self._request(
            "MKCOL",
            path,
            expected_statuses=[201, 405, 409, 507],
        )
        if status == 405 and not exist_ok:
            raise WebDAVMethodNotAllowedError(
                "Failed to create directory, already exists. "
                + get_error_details(response_data),
            )
        elif status == 409:
            raise WebDAVConflictError(
                "Failed to create directory: parent does not exist. "
                + get_error_details(response_data),
            )
        elif status == 507:
            raise WebDAVInsufficientStorageError(
                "Failed to create directory: insufficient storage. "
                + get_error_details(response_data),
            )

    async def copy(self, src_path: str, dst_path: str, overwrite: bool = True) -> None:
        """
        Copy a resource from src_path to dst_path.

        Arguments:
            src_path: Source path of the resource to copy.
            dst_path: Destination path where the resource will be copied.
            overwrite: If True, overwrite the destination if it exists. Default is True.

        Returns:
            None

        Raises:
            WebDAVMultiStatusError: If the COPY operation partially fails.
            WebDAVHttpError: Other HTTP errors.
            aiohttp.ClientError: Network-related errors.
        """
        headers = {
            "Destination": self.base_path + "/" + dst_path.lstrip("/"),
            "Depth": "infinity",
            "Overwrite": "T" if overwrite else "F",
        }
        status, response_data = await self._request(
            "COPY",
            src_path,
            headers=headers,
            expected_statuses=[201, 204, 207],
        )
        if status == 207:
            responses = parse_move_copy_multistatus_response(response_data)
            for resp in responses.values():
                if resp.get("status") not in [201, 204]:
                    raise WebDAVMultiStatusError(
                        "COPY operation partially failed.", responses
                    )

    async def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> None:
        """
        Move a resource from src_path to dst_path.

        Arguments:
            src_path: Source path of the resource to move.
            dst_path: Destination path where the resource will be moved.
            overwrite: If True, overwrite the destination if it exists. Default is True.

        Returns:
            None

        Raises:
            WebDAVHttpError: If the MOVE operation fails.
            aiohttp.ClientError: Network-related errors.
        """
        headers = {
            "Destination": self.base_path + "/" + dst_path.lstrip("/"),
            "Depth": "infinity",
            "Overwrite": "T" if overwrite else "F",
        }
        status, response_data = await self._request(
            "MOVE",
            src_path,
            headers=headers,
            expected_statuses=[201, 204, 207],
        )
        if status == 207:
            responses = parse_move_copy_multistatus_response(response_data)
            for resp in responses.values():
                if resp.get("status") not in [201, 204]:
                    raise WebDAVMultiStatusError(
                        "MOVE operation partially failed.", responses
                    )

    async def delete(self, path: str, missing_ok: bool = False) -> None:
        """
        Delete a file or directory.

        Directories are deleted recursively.

        Arguments:
            path: The path of the resource to delete.
            missing_ok: If True, do not raise an error if the resource does not exist. Default is False.

        Returns:
            None

        Raises:
            WebDAVNotFoundError: The resource does not exist (if missing_ok is False).
            WebDAVHttpError: The resource cannot be deleted.
            aiohttp.ClientError: Network-related errors.
        """
        status, _ = await self._request(
            "DELETE",
            path,
            expected_statuses=[204, 404],
        )
        if status == 404 and not missing_ok:
            raise WebDAVNotFoundError(f"Resource not found: {path}")

    @asynccontextmanager
    async def lock(
        self,
        path: str,
        owner: str | None = None,
        timeout: int | None = None,
        scope: str = "exclusive",
        type_: str = "write",
    ) -> AsyncGenerator[WebDAVLockInfo, None]:
        """
        Lock a resource.

        Arguments:
            path: The path of the resource to lock.
            owner: Optional owner string to include in the lock.
            timeout: Optional timeout in seconds for the lock. If None, server default is used.
            scope: Lock scope, either "exclusive" or "shared". Default is "exclusive".
            type_: Lock type, currently only "write" is supported. Default is "write".

        Yields:
            A dictionary with lock information, including lock token and details.

        Raises:
            WebDAVConflictError: If the parent resource does not exist.
            WebDAVLockedError: If the resource is already locked.
            WebDAVHttpError: Other HTTP errors.
            aiohttp.ClientError: Network-related errors.
            ValueError: If invalid scope or type_ is provided.
        """
        lock_xml = create_lock_xml(owner=owner, scope=scope, type_=type_)
        headers = {"Content-Type": "application/xml; charset=utf-8"}
        if timeout is not None:
            headers["Timeout"] = f"Second-{timeout}"

        async with self._request_context(
            "LOCK",
            path,
            headers=headers,
            data=lock_xml,
            expected_statuses=[200, 201, 409, 423],
        ) as response:
            if response.status == 409:
                raise WebDAVConflictError(
                    "Failed to obtain lock: parent missing. "
                    + get_error_details(await response.read())
                )
            if response.status == 423:
                raise WebDAVLockedError(
                    "Failed to obtain lock: resource is locked. "
                    + get_error_details(await response.read())
                )
            lock_token = response.headers.get("Lock-Token", "").strip("<>")
            if not lock_token:
                raise WebDAVError("No Lock-Token header in response")
            lock_info = parse_lock_response(await response.read())
        root = lock_info.get("lockroot")
        if not root:
            root = path
        else:
            root = root.removeprefix(self.base_path)
        lock = WebDAVLockInfo(
            token=lock_info.get("locktoken", lock_token),
            root=root,
            scope=lock_info.get("scope", scope),
            type=lock_info.get("type", type_),
            owner=lock_info.get("owner", owner),
            timeout=lock_info.get("timeout", timeout),
            depth=lock_info.get("depth", "infinity"),
        )
        try:
            yield lock
        finally:
            # Unlock the resource
            unlock_headers = {"Lock-Token": f"<{lock.token}>"}
            await self._request(
                "UNLOCK",
                path,
                headers=unlock_headers,
                expected_statuses=[204],
            )

    async def refresh_lock(
        self,
        lock: WebDAVLockInfo,
    ) -> WebDAVLockInfo:
        """
        Refresh an existing lock.

        Updates the lock details in the provided WebDAVLockInfo object in place.

        Arguments:
            lock: The existing WebDAVLockInfo object representing the lock to refresh.

        Returns:
            The original WebDAVLockInfo object with updated lock details.

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        headers = {"If": f"(<{lock.token}>)"}
        if lock.timeout:
            headers["Timeout"] = lock.timeout
        _, response_data = await self._request(
            "LOCK",
            lock.root,
            headers=headers,
            expected_statuses=[200],
        )
        lock_info = parse_lock_response(response_data)
        lock.token = lock_info.get("locktoken", lock.token)
        if lock_info.get("lockroot"):
            lock.root = lock_info["lockroot"].removeprefix(self.base_path)
        lock.scope = lock_info.get("scope", lock.scope)
        lock.type = lock_info.get("type", lock.type)
        lock.owner = lock_info.get("owner", lock.owner)
        lock.timeout = lock_info.get("timeout", lock.timeout)
        lock.depth = lock_info.get("depth", lock.depth)
        return lock

    async def download_file(self, remote_path: str, local_path: str | Path) -> None:
        """
        Download file to local filesystem.

        Arguments:
            remote_path: The path of the remote file to download.
            local_path: The local filesystem path to save the file to.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
            OSError for local file write errors.
        """
        local_path = Path(local_path)
        async with self._request_context("GET", remote_path) as response:
            with open(local_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1024):
                    f.write(chunk)

    async def download_bytes(self, remote_path: str) -> bytes:
        """
        Download file as bytes.

        Arguments:
            remote_path: The path of the remote file to download.

        Returns:
            The file content as bytes.

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        async with self._request_context("GET", remote_path) as response:
            return await response.read()

    async def download_text(
        self, remote_path: str, encoding: str = "utf-8", errors: str = "ignore"
    ) -> str:
        """
        Download file as text.

        Arguments:
            remote_path: The path of the remote file to download.
            encoding: The text encoding to use (default: 'utf-8').
            errors: Error handling scheme (default: 'ignore').

        Returns:
            The file content as a string.

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        async with self._request_context("GET", remote_path) as response:
            text = await response.text(encoding=encoding, errors=errors)
            return text

    async def download_json(self, remote_path: str) -> Any:
        """
        Download file as JSON.

        Arguments:
            remote_path: The path of the remote file to download.

        Returns:
            The parsed JSON content.

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
            json.JSONDecodeError if the content is not valid JSON.
        """
        async with self._request_context("GET", remote_path) as response:
            return await response.json(content_type=None)

    @asynccontextmanager
    async def download_stream(
        self, remote_path: str
    ) -> AsyncGenerator[aiohttp.StreamReader, None]:
        """
        Download file as stream.

        Arguments:
            remote_path: The path of the remote file to download.

        Yields:
            An asynchronous stream reader for the file content.
            See the aiohttp.StreamReader documentation for usage details.

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        async with self._request_context("GET", remote_path) as response:
            yield response.content

    async def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        content_type: str | None = None,
        lock_token: str | None = None,
    ) -> None:
        """
        Upload file from local filesystem.

        Arguments:
            local_path: The local filesystem path of the file to upload.
            remote_path: The path on the server to upload the file to.
            content_type: Optional Content-Type header value. Default is None.
            lock_token: Optional lock token if the resource is locked.

        Returns:
            None

        Raises:
            ValueError: If local_path is not a file.
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
            OSError for local file read errors.
        """
        local_path = Path(local_path)
        if not local_path.is_file():
            raise ValueError(f"Local path is not a file: {local_path}")
        with open(local_path, "rb") as f:
            data = f.read()
            await self._put(remote_path, data, lock_token)

    async def upload_bytes(
        self,
        data: bytes,
        remote_path: str,
        content_type: str | None = "application/octet-stream",
        lock_token: str | None = None,
    ) -> None:
        """
        Upload bytes data.

        Arguments:
            data: The bytes data to upload.
            remote_path: The path on the server to upload the data to.
            content_type: Optional Content-Type header value. Default is None.
            lock_token: Optional lock token if the resource is locked.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        await self._put(remote_path, data, content_type, lock_token)

    async def upload_text(
        self,
        text: str,
        remote_path: str,
        content_type: str | None = "text/plain; charset=utf-8",
        lock_token: str | None = None,
    ) -> None:
        """
        Upload text data.

        Arguments:
            text: The text data to upload.
            remote_path: The path on the server to upload the data to.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        await self._put(remote_path, text, content_type, lock_token)

    async def upload_json(
        self,
        data: Any,
        remote_path: str,
        content_type: str | None = "application/json; charset=utf-8",
        lock_token: str | None = None,
    ) -> None:
        """
        Upload JSON data.

        Arguments:
            data: The data to serialize as JSON and upload.
            remote_path: The path on the server to upload the data to.
            content_type: Optional Content-Type header value. Default is None.
            lock_token: Optional lock token if the resource is locked.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
            TypeError if the data is not JSON serializable.
        """
        await self._put(remote_path, json.dumps(data), content_type, lock_token)

    async def upload_stream(
        self,
        stream: BinaryIO,
        remote_path: str,
        content_type: str | None = "application/octet-stream",
        lock_token: str | None = None,
    ) -> None:
        """
        Upload from stream.

        Arguments:
            stream: A file-like object opened in binary mode.
            remote_path: The path on the server to upload the data to.
            content_type: Optional Content-Type header value. Default is None.
            lock_token: Optional lock token if the resource is locked.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
            OSError for local stream read errors.
        """
        data = stream.read()
        await self._put(remote_path, data, content_type, lock_token)

    async def _put(
        self,
        path: str,
        data: bytes | str | BinaryIO,
        content_type: str | None = None,
        lock_token: str | None = None,
    ) -> None:
        """
        Perform a PUT request to upload data.

        Arguments:
            path: The URL path relative to base_url.
            data: The data to upload (bytes, str, or file-like object).
            content_type: Optional Content-Type header value. Default is None.
            lock_token: Optional lock token if the resource is locked.

        Returns:
            None

        Raises:
            WebDAVHttpError or its subclasses for various error conditions.
            aiohttp.ClientError for network-related errors.
        """
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        if lock_token:
            headers["If"] = (
                f"<{self.base_url + '/' + path.lstrip('/')}> (<{lock_token}>)"
            )

        status, response_data = await self._request(
            "PUT",
            path,
            headers=headers,
            data=data,
            expected_statuses=[200, 201, 204, 405, 409, 507],
        )
        if status == 405:
            raise WebDAVMethodNotAllowedError(
                "Failed to upload, resource is a directory. "
                + get_error_details(response_data),
            )
        elif status == 409:
            raise WebDAVConflictError(
                "Failed to upload: parent does not exist. "
                + get_error_details(response_data),
            )
        elif status == 507:
            raise WebDAVInsufficientStorageError(
                "Failed to upload: insufficient storage. "
                + get_error_details(response_data),
            )
