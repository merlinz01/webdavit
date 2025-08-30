# WebDAVit

WebDAVit is a simple async WebDAV client for Python.

## Features

- Basic file operations: list, upload, download, mkdir, delete, move, copy, lock.
- Support for authentication.
- Asynchronous operations using `aiohttp` for non-blocking I/O.
- Lightweight and easy to use.

## Installation

You can install WebDAVit using pip:

```bash
pip install webdavit
```

Or, if you're using `uv`, you can add it with:

```bash
uv add webdavit
```

## Usage

Here's a quick overview of how to use WebDAVit:

```python
import asyncio
import io
from webdavit import WebDAVClient

async def example():
    async with WebDAVClient(
            'https://webdav.example.com',
            username='user',
            password='pass',
        ) as client:

        # List files in the root directory
        files = await client.list('/')
        print(files)

        # Upload a file
        await client.upload_file('/path/to/local_file.txt', '/remote_file.txt')
        await client.upload_text('Hello, WebDAV!', '/hello.txt')
        await client.upload_bytes(b'Hello, WebDAV in bytes!', '/hello_bytes.txt')
        await client.upload_stream(io.BytesIO(b'Streaming data!'), '/stream.txt')
        await client.upload_json({'key': 'value'}, '/data.json')

        # Download a file
        await client.download_file('/remote_file.txt', '/path/to/local_file.txt')
        text = await client.download_text('/hello.txt')
        print(text)
        jsondata = await client.download_json('/data.json')
        print(jsondata)
        bytesdata = await client.download_bytes('/hello_bytes.txt')
        print(bytesdata)
        async with client.download_stream('/stream.txt')
            print(await stream.read())

        # Delete a file
        await client.delete('/remote_file.txt')

        # Create a directory
        await client.mkdir('/new_directory')

        # Delete a directory
        # Note that this is a recursive delete
        await client.delete('/new_directory')

        # Move or rename a file
        await client.move('/old_name.txt', '/new_name.txt')

        # Copy a file
        await client.copy('/source.txt', '/destination.txt')

        # Check if a file or directory exists
        exists = await client.exists('/some_path')
        print(f'Exists: {exists}')

        # Get file or directory info
        info = await client.get('/some_path')
        print(info)

        # Lock a file
        async with await client.lock('/some_file.txt', timeout=60) as lock:
            print(f'Lock info: {lock}')
            await client.upload_text(
                'Updated content while locked',
                '/some_file.txt',
                lock_token=lock.token
            )
            # Refresh the lock if needed
            await client.refresh_lock(lock)
            print(f'Refreshed lock info: {lock}')
            # The lock will be released automatically when exiting the context

asyncio.run(example())
```

## License

WebDAVit is licensed under the MIT License.
See the [LICENSE.txt](/LICENSE.txt) file for details.
