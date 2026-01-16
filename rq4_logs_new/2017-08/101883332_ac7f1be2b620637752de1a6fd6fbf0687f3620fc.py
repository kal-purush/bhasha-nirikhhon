#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" An example of advanced mutaprops functions:

* parameter grouping
* mutasources
* custom styling
* static files - images in the docstrings
* raw HTML
* asyncio integration
* multiple objects
* custom help

Note that this example uses files in the ``assets/`` directory.


Credits:

Hoovercraft logo created by Theresa Stoodley from the Noun Project.
Licensed under Creative Commons 3.0

"""

# TODO: class-level mutasources and mutaprops example

from mutaprops import *
from mutaprops.utils import rest_to_html
import asyncio
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
        self._steering_locked = True
        self._eel_display = None
        self._set_eel_display(self._eel_count)

    # Following is the example of raw HTML support. It's basically an
    # attribute containing HTML code.
    # In case of change, the entier code is forwarded over websocket,
    # so it's not very efficient solution. Some upgrades are planned.
    @mutaproperty("Eel loading process",
                  MutaTypes.HTML,
                  hierarchy='Eel control')  # Note the hierarchy argument, explained below
    def eel_display(self):
        return self._eel_display

    @eel_display.setter
    def eel_display(self, value):
        self._eel_display = value

    def _set_eel_display(self, value):
        """ Wrapper for Raw HTML property setting."""
        self.eel_display = """<div class="progress">
            <div class="progress-bar" role="progressbar" aria-valuenow="{0}" 
            aria-valuemin="0" aria-valuemax="100"
             style="min-width: 2em; width: {0}%;"> {0}% </div>
            </div>
        """.format((value/self.MAX_EELS)*100)

    # Hierarchy allows grouping of parameters into an UI panels
    # Panels can be minimized to save space (only in UI)
    # Hierarchy can be flat (like in this example) or can have sub-sections
    # like "Eel control/loading" etc. (then panel in panel would be created)
    @mutaproperty("Number of eels", MutaTypes.INT,
                  min_val=0, max_val=MAX_EELS,
                  hierarchy='Eel control')
    def eels(self):
        return self._eel_count

    @eels.setter
    def eels(self, value):
        self._eel_count = value
        self._set_eel_display(self._eel_count)
        if self._eel_count >= self.MAX_EELS:
            logger.warning("The hoovercraft is full of eels!")

    @mutaprop_action("Drop all eels!", hierarchy='Eel control')
    def drop_all_eels(self):
        self.eels = 0
        logger.info("Eels are goooone!")


    # Autodocumentation is generated from the function docstrings.
    # On UI level, it makes the name of the mutapropert clickable,
    # The documentation is then expanded upon clicking on the mutaprop name.
    @mutaproperty("Engine state", MutaTypes.BOOL,
                  toggle={'on': 'Running', 'off': 'Stopped'},
                  hierarchy='Hoovercraft control')
    def engine(self):
        """ Hoovercraft engine is very very noisy.

        Can be set to **running** or **stopped**.

        .. image:: local/hoovercraft-scheme.png

        Image by
        `MesserWolland <https://commons.wikimedia.org/wiki/User:MesserWoland>`_
        licensed under
        `CC BY-SA 3.0 <http://creativecommons.org/licenses/by-sa/3.0/>`_.
        """
        return self._engine_running

    @engine.setter
    def engine(self, value):
        self._engine_running = value
        self.steering_locked = not value
        logger.info("Engine %s",
                    'started' if self._engine_running else 'stopped')

    @mutasource
    def steering_locked(self):
        return self._steering_locked

    @steering_locked.setter
    def steering_locked(self, value):
        self._steering_locked = value

    @mutaproperty("Speed [km/h]", MutaTypes.INT, min_val=0, max_val=80,
                  read_only=steering_locked,
                  hierarchy='Hoovercraft control')
    def speed(self):
        return self._speed

    @speed.setter
    def speed(self, value):
        self._speed = value
        logger.info("Speed set to %d km/h", self._speed)

    @mutaproperty("Direction of travel", MutaTypes.STRING,
                  select=['North', 'East', 'South', 'West'],
                  read_only=steering_locked,
                  hierarchy='Hoovercraft control')
    def direction(self):
        return self._direction

    @direction.setter
    def direction(self, value):
        self._direction = value
        logger.info("Going %s", self._direction)


custom_help = """

Hoovercraft manager 
-------------------

This is an example help text, accessible through the help link in the
right top corner.

.. image:: http://i.imgur.com/WlOrqdR.jpg


It's ReST formatted, so it needs to be converted to HTML first.
use ``mutaprops.utils.rest_to_html`` for that.

"""

@asyncio.coroutine
def update_eels(obj):
    while True:
        obj.eels += 1
        yield from asyncio.sleep(1)
        if obj.eels > obj.MAX_EELS:
            obj.drop_all_eels()
            yield from asyncio.sleep(1)


if __name__ == '__main__':

    test = Hoovercraft()
    test.muta_init("Hoovercraft #1")  # Initializes the mutaproperties.
    test2 = Hoovercraft()

    # Example of some asyncio stuff
    loop = asyncio.get_event_loop()

    loop.create_task(update_eels(test))

    man = HttpMutaManager("Hoovercraft manager", proxy_log=logger,
                          loop=loop,  # UI manager will run in app's loop.
                          local_dir="assets",
                          help_doc=rest_to_html(custom_help))
    man.add_object(test)

    man.add_object(test2, "Hoovercraft #2")  # We can also muta-init object while adding, just by specifying the ID as the argument.

    man.run(port=9000)
