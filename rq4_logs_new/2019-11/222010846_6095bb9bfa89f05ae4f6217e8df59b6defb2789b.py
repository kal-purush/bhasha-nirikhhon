#!/usr/bin/env python3
# encoding: utf-8

from enum import Enum
from io import BytesIO

from picamera import PiCamera

from cam_handling import get_camera_options, set_to_night_vision


class CameraManager:

    class State(Enum):
        idle = 0
        taking_a_picture = 1

    def __init__(self):
        self.state = self.State.idle
        self._camera = None

        self.picture_processors = []

    def get_camera(self):
        if self._camera is None:
            options = self.get_camera_options()
            self._camera = PiCamera(**options)
            set_to_night_vision(self._camera)
        return self._camera

    def get_camera_options(self):
        return get_camera_options()

    def cleanup(self):
        if self._camera is not None:
            self._camera.close()

    def add_picture_processor(self, processor):
        self.picture_processors.append(processor)

    async def when_motion(self):
        print("when motion")
        return await self.take_a_picture()

    async def when_no_motion(self):
        print("when no motion")

    async def take_a_picture(self):
        if self.state != self.State.idle:
            return False
        self.state = self.State.taking_a_picture

        camera = self.get_camera()
        content = self._take_a_picture(camera)
        await self.process_picture(content)
        self.state = self.State.idle
        return True

    async def process_picture(self, content):
        for pp in self.picture_processors:
            await pp.process(content)

    def _take_a_picture(self, camera):
        try:
            content = BytesIO()
            camera.capture(content, format='jpeg')
            return content.getvalue()
        finally:
            content.close()