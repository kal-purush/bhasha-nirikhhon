#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime
import os
from os.path import dirname, join, realpath

from aiohttp import web


_current_dir = realpath(dirname(__file__))
MEDIA_DIR = join(_current_dir, 'media')


def setup():
    # make sure media dir exists
    os.makedirs(MEDIA_DIR, 0o770, exist_ok=True)


async def picture_upload(request):
    """
    Receives an image and writes it down to disk.
    """
    reader = await request.multipart()
    field = await reader.next()
    filename = str(datetime.now()).replace(" ", "_") + '.jpg'
    with open(os.path.join(MEDIA_DIR, filename), 'wb') as f:
        while True:
            chunk = await field.read_chunk()  # 8192 bytes by default.
            if not chunk:
                break
            f.write(chunk)

    return web.Response(text='Ok')


if __name__ == "__main__":
    setup()
    app = web.Application()
    app.router.add_routes([
        web.post('/picture', picture_upload),
    ])

    web.run_app(app, port=8075)