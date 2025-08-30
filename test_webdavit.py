import os
import socket
import subprocess
import time

import pytest

from webdavit import WebDAVClient, WebDAVResource
from webdavit.client import WebDAVLockInfo
from webdavit.exceptions import WebDAVNotFoundError

# Choose which WebDAV server to use for testing.
# Options: "nextcloud", "hacdias", or "all" to run tests against both.
# Nextcloud takes longer to start. It doesn't support locking, so those tests will be skipped.
# hacdias/webdav is lightweight and fast, written in Go, and supports locking.
# Ensure Docker is installed and running, as these tests use Docker containers.
_webdav_server = os.getenv("WEBDAV_SERVER", "hacdias").lower()
_webdav_server_options = ("nextcloud", "hacdias")
_webdav_servers = []
if _webdav_server == "all":
    _webdav_servers = _webdav_server_options
elif _webdav_server in _webdav_server_options:
    _webdav_servers = [_webdav_server]
else:
    raise ValueError(
        "WEBDAV_SERVER environment variable must be one of "
        + ", ".join(_webdav_server_options)
        + " or 'all'",
    )


@pytest.fixture(scope="session", params=_webdav_servers)
def webdav_server(tmp_path_factory: pytest.TempPathFactory, request):
    server = request.param
    if server == "nextcloud":
        print("Using Nextcloud for WebDAV tests")
        cmd = [
            "docker",
            "container",
            "create",
            "--rm",
            "-e",
            "NEXTCLOUD_ADMIN_USER=admin",
            "-e",
            "NEXTCLOUD_ADMIN_PASSWORD=12345",
            "-e",
            "SQLITE_DATABASE=sqlite.db",
            "-p",
            "8080:80",
            "--name",
            "webdavit-test",
            "nextcloud:latest",
        ]
    elif server == "hacdias":
        print("Using hacdias/webdav for WebDAV tests")
        tmp_path = tmp_path_factory.mktemp("webdavit-test-data")
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """
tls: false
address: 0.0.0.0
port: 8080
prefix: /remote.php/dav/files/admin
debug: true
directory: /data
users:
    - username: admin
      password: 12345
      permissions: CRUD
    """,
            encoding="utf-8",
        )
        data_path = tmp_path / "data"
        data_path.mkdir()
        cmd = [
            "docker",
            "container",
            "create",
            "--rm",
            "-p",
            "8080:8080",
            "-v",
            f"{data_path}:/data",
            "-v",
            f"{config_file}:/config.yml:ro",
            "--name",
            "webdavit-test",
            "hacdias/webdav:latest",
        ]
    else:
        raise ValueError("Unrecognized WEBDAV_SERVER value")
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0 or "Conflict" in p.stderr:
        raise RuntimeError(f"Failed to create WebDAV container: {p.stderr}")

    cmd = ["docker", "start", "webdavit-test"]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        subprocess.run(["docker", "rm", "--force", "webdavit-test"])
        raise RuntimeError(f"Failed to start WebDAV container: {p.stderr}")

    # Wait for server to be ready
    for _ in range(10):
        time.sleep(1)
        try:
            with socket.create_connection(("localhost", 8080), timeout=1):
                break
        except (ConnectionRefusedError, socket.timeout):
            print("Waiting for WebDAV server to start...")
    else:
        subprocess.run(["docker", "rm", "--force", "webdavit-test"])
        raise RuntimeError("Failed to start WebDAV server in Docker")
    if server == "nextcloud":
        time.sleep(7)  # Allow Nextcloud some extra time to initialize
    try:
        yield server
    finally:
        p = subprocess.run(
            ["docker", "logs", "webdavit-test"],
            capture_output=True,
            text=True,
        )
        print(f"WebDAV container logs:\n{p.stdout}\n{p.stderr}")
        subprocess.run(["docker", "rm", "--force", "webdavit-test"])


@pytest.fixture
def webdav_client(webdav_server):
    """Fixture providing a configured WebDAV client"""
    return WebDAVClient(
        "http://localhost:8080/remote.php/dav/files/admin",
        username="admin",
        password="12345",
        timeout=5,
        trust_env=False,
    )


@pytest.mark.asyncio
async def test_basic_connection(webdav_client: WebDAVClient):
    """Test basic connection to WebDAV server"""

    async with webdav_client:
        response = await webdav_client.list("/")
        assert isinstance(response, list)


@pytest.mark.asyncio
async def test_client_context_manager(webdav_client: WebDAVClient):
    """Test client can be used as async context manager"""

    async with webdav_client:
        assert webdav_client._session is not None

    assert webdav_client._session is None


@pytest.mark.asyncio
async def test_exists(webdav_client: WebDAVClient):
    """Test checking if resource exists"""

    async with webdav_client:
        assert await webdav_client.exists("/")
        assert not await webdav_client.exists("/nonexistent.txt")


@pytest.mark.asyncio
async def test_mkdir_and_delete(webdav_client: WebDAVClient):
    """Test creating and deleting directories"""
    async with webdav_client:
        await webdav_client.mkdir("/testdir")
        assert await webdav_client.exists("/testdir")
        await webdav_client.mkdir("/testdir/nested")
        assert await webdav_client.exists("/testdir/nested")
        await webdav_client.delete("/testdir/nested")
        assert not await webdav_client.exists("/testdir/nested")
        await webdav_client.delete("/testdir")
        assert not await webdav_client.exists("/testdir")


@pytest.mark.asyncio
async def test_upload_download_text(webdav_client: WebDAVClient):
    """Test uploading and downloading text data"""
    test_data = "Hello, WebDAV! This is a test. 漢字"

    async with webdav_client:
        await webdav_client.upload_text(test_data, "/test.txt")
        assert await webdav_client.exists("/test.txt")
        downloaded = await webdav_client.download_text("/test.txt")
        assert downloaded == test_data
        await webdav_client.delete("/test.txt")


@pytest.mark.asyncio
async def test_upload_download_bytes(webdav_client: WebDAVClient):
    """Test uploading and downloading binary data"""
    test_data = b"\x00\x01\x02\x03\xff\xfe\xfd"

    async with webdav_client:
        # Upload bytes
        await webdav_client.upload_bytes(test_data, "/test.bin")
        assert await webdav_client.exists("/test.bin")

        # Download and verify
        downloaded = await webdav_client.download_bytes("/test.bin")
        assert downloaded == test_data

        # Clean up
        await webdav_client.delete("/test.bin")


@pytest.mark.asyncio
async def test_upload_download_json(webdav_client: WebDAVClient):
    """Test uploading and downloading JSON data"""
    test_data = {"message": "Hello", "numbers": [1, 2, 3], "nested": {"key": "value"}}

    async with webdav_client:
        # Upload JSON
        await webdav_client.upload_json(test_data, "/test.json")
        assert await webdav_client.exists("/test.json")

        # Download and verify
        downloaded = await webdav_client.download_json("/test.json")
        assert downloaded == test_data

        # Clean up
        await webdav_client.delete("/test.json")


@pytest.mark.asyncio
async def test_list_directory_contents(webdav_client: WebDAVClient):
    """Test listing directory contents"""

    async with webdav_client:
        # Create some test files and directories
        await webdav_client.upload_text("file1 content", "/file1.txt")
        await webdav_client.upload_text("file2 content", "/file2.txt")
        await webdav_client.mkdir("/subdir1")

        # List root directory
        contents = await webdav_client.list("/")

        # Should contain our created items
        assert isinstance(contents, list)
        assert all(isinstance(item, WebDAVResource) for item in contents)
        names = [item.name for item in contents]
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "subdir1" in names

        # Clean up
        await webdav_client.delete("/file1.txt")
        await webdav_client.delete("/file2.txt")
        await webdav_client.delete("/subdir1")


@pytest.mark.asyncio
async def test_copy(webdav_client: WebDAVClient):
    """Test copying resources"""
    original_content = "Original file content"

    async with webdav_client:
        await webdav_client.upload_text(original_content, "/original.txt")
        await webdav_client.copy("/original.txt", "/copy.txt")
        assert await webdav_client.exists("/copy.txt")
        copied_content = await webdav_client.download_text("/copy.txt")
        assert copied_content == original_content
        await webdav_client.delete("/original.txt")
        await webdav_client.delete("/copy.txt")


@pytest.mark.asyncio
async def test_move(webdav_client: WebDAVClient):
    """Test moving/renaming resources"""
    original_content = "File to be moved"
    async with webdav_client:
        await webdav_client.upload_text(original_content, "/original.txt")
        assert await webdav_client.exists("/original.txt")
        await webdav_client.move("/original.txt", "/moved.txt")
        assert not await webdav_client.exists("/original.txt")
        assert await webdav_client.exists("/moved.txt")
        moved_content = await webdav_client.download_text("/moved.txt")
        assert moved_content == original_content
        await webdav_client.delete("/moved.txt")


@pytest.mark.asyncio
async def test_get_resource_properties(webdav_client: WebDAVClient):
    """Test getting resource metadata/properties"""
    test_content = "Test file for properties"

    async with webdav_client:
        await webdav_client.upload_text(test_content, "/props_test.txt")
        assert await webdav_client.exists("/props_test.txt")
        props = await webdav_client.get("/props_test.txt")
        assert isinstance(props, WebDAVResource)
        assert props.name == "props_test.txt"
        assert props.size == len(test_content)
        assert props.is_dir is False
        await webdav_client.delete("/props_test.txt")


@pytest.mark.asyncio
async def test_client_url_construction():
    """Test client URL handling"""
    client1 = WebDAVClient("http://somehost/webdav/")
    assert client1.base_url == "http://somehost/webdav"
    client2 = WebDAVClient("http://somehost/webdav")
    assert client2.base_url == "http://somehost/webdav"
    with pytest.raises(ValueError):
        WebDAVClient("not a url")


@pytest.mark.asyncio
async def test_error_handling(webdav_client: WebDAVClient):
    """Test various error conditions"""
    async with webdav_client:
        with pytest.raises(WebDAVNotFoundError):
            await webdav_client.download_text("/nonexistent.txt")
        with pytest.raises(WebDAVNotFoundError):
            await webdav_client.get("/nonexistent.txt")


@pytest.mark.asyncio
async def test_locking(webdav_client: WebDAVClient, webdav_server: str):
    """Test locking and unlocking resources"""
    if webdav_server == "nextcloud":
        pytest.skip("Nextcloud does not support WebDAV locking")

    test_content = "Lock test content"

    async with webdav_client:
        await webdav_client.upload_text(test_content, "/lock_test.txt")
        assert await webdav_client.exists("/lock_test.txt")

        # Lock the resource
        async with webdav_client.lock("/lock_test.txt", timeout=30) as lock:
            assert isinstance(lock, WebDAVLockInfo)
            # Try to upload without lock token - should fail
            with pytest.raises(Exception):
                await webdav_client.upload_text("New content", "/lock_test.txt")
            # Upload with lock token - should succeed
            await webdav_client.upload_text(
                "New content with lock",
                "/lock_test.txt",
                lock_token=lock.token,
            )
            content = await webdav_client.download_text("/lock_test.txt")
            assert content == "New content with lock"
            # Refresh lock
            await webdav_client.refresh_lock(lock)
            # Make sure lock is still valid
            await webdav_client.upload_text(
                "Updated content with refreshed lock",
                "/lock_test.txt",
                lock_token=lock.token,
            )
            content = await webdav_client.download_text("/lock_test.txt")
            assert content == "Updated content with refreshed lock"

        # After exiting lock context, should be able to upload without token
        await webdav_client.upload_text("Content after unlock", "/lock_test.txt")
        content = await webdav_client.download_text("/lock_test.txt")
        assert content == "Content after unlock"

        # Clean up
        await webdav_client.delete("/lock_test.txt")
