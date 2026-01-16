#!/usr/bin/env python3

from spockbot import Client
from spockbot.plugins import default_plugins

from piuda.piuda_plugin import PiudaPlugin

plugins = default_plugins + [('piuda', PiudaPlugin)]

client = Client(plugins=plugins, settings={
    'start': {
        'username': 'Piuda'
    },
    'auth': {
        'authenticated': False
    }
})

client.start('wurstmineberg.de', 25563)