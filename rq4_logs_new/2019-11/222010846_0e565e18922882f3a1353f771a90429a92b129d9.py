#!/usr/bin/env python3
# encoding: utf-8

from datetime import datetime
import os
from os.path import dirname, join, realpath

import aiohttp


class SaveToDiskPictureProcessor:

    def __init__(self, is_active=True):
        self.is_active = is_active
        self.media_dir = join(realpath(dirname(__file__)), 'media')

    def process(self, content):
        if not self.is_active:
            return None

        pth = join(self.media_dir, self.get_filename())
        with open(pth, 'bw') as f:
            f.write(content)

    @staticmethod
    def get_filename():
        return str(datetime.now()).replace(" ", "_") + '.jpg'


class SendToKotomeraPictureProcessor:

    def __init__(self):
        self.url = os.environ['KOTOMERA_URL']

    async def process(self, content):
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field('file', content, filename='whatever.jpg',
                           content_type='image/jpeg')

            async with session.post(self.url, data=data) as resp:
                print(resp.status)
                print(await resp.text())