#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mutaprops import *
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

logger = logging.getLogger(__name__)

@mutaprop_class("Hoovercraft UI")
class Hoovercraft:

    MAX_EELS = 40

    def __init__(self, number_of_eels=20, speed=0, direction='North'):
        self._eel_count = number_of_eels
        self._speed = speed
        self._direction = direction
        self._engine_running = False

    @mutaproperty("Number of eels", MutaTypes.INT, min_val=0, max_val=MAX_EELS)
    def eels(self):
        return self._eel_count

    @eels.setter
    def eels(self, value):
        self._eel_count = value
        if self._eel_count >= self.MAX_EELS:
            logger.warning("The hoovercraft is full of eels!")

    @mutaprop_action("Drop all eels!")
    def drop_all_eels(self):
        self.eels = 0
        logger.info("Eels are goooone!")

    @mutaproperty("Engine state", MutaTypes.BOOL,
                  toggle={'on': 'Running', 'off': 'Stopped'})
    def engine(self):
        return self._engine_running

    @engine.setter
    def engine(self, value):
        self._engine_running = value

    @mutaproperty("Speed [km/h]", MutaTypes.INT, min_val=0, max_val=80)
    def speed(self):
        return self._speed

    @speed.setter
    def speed(self, value):
        self._speed = value
        logger.info("Speed set to %d km/h", self._speed)

    @mutaproperty("Direction of travel", MutaTypes.STRING,
                  select=['North', 'East', 'South', 'West'])
    def direction(self):
        return self._direction

    @direction.setter
    def direction(self, value):
        self._direction = value
        logger.info("Going %s", self._direction)


if __name__ == '__main__':

    test = Hoovercraft()
    test.muta_init("Hoovercraft instance #1")
    man = HttpMutaManager("Hoovercraft manager", proxy_log=logger)
    man.add_object(test)
    man.run(port=9000)

