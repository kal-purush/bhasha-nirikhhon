#! /usr/bin/env python

#
#    Modular REST API dispatcher in Python (dispytch)
#
#    Copyright (C) 2015 Denis Pompilio (jawa) <denis.pompilio@gmail.com>
#    Copyright (C) 2015 Cyrielle Camanes (cycy) <cyrielle.camanes@gmail.com>
#
#    This file is part of dispytch
#
#    This program is free software; you can redistribute it and/or
#    modify it under the terms of the GNU General Public License
#    as published by the Free Software Foundation; either version 2
#    of the License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, see <http://www.gnu.org/licenses/>.

"""Munin RRD requesting module

Use this API to extract and handle informations from Munin datafiles (rrd).
This API support both POST and GET method to handle queries. GET requests can
be formatted using document path or url-encoded format.

Usage:
    Using document path:
      /munin/list[/<ip>]
      /munin/list[/templates]
      /munin/by-ip/<ip>/<datatype>/<cf>/<start>/<stop>[/<template>]
      /munin/by-id/<id>/<datatype>/<cf>/<start>/<stop>[/<template>]
    Using url-encoded request:
      /munin/list[?ip=<ip>]
      /munin/by-ip?ip=<ip>&datatype=<datatype>&...

Known Fields:
    ip              IP address of the target used by Munin
    id              Munin entry name in configuration
    datatype        Munin datatype as shown in RRDs names
    cf              RRD consolidation function to use
    start           Start time as supported by RRD library
    stop            Stop time as supported by RRD library
    template        Template to use for returned datas structuration

Exemple:
    /munin/by-ip/1.1.1.1/cpu/AVERAGE/now-2h/now
    /munin/by-id/munin;config;id/processes/AVERAGE/1383260400/138585240
"""


import os
import logging
import ConfigParser

from . import infos, rrd_utils, requests


_log = logging.getLogger("dispytch")

DATADIR = None


def selfcheck(config):
    """Selfcheck module functionnalities

    :param dict config: Configuration informations
    """
    try:
        assert config.has_key('config')
        assert config.has_key('datadir')
        assert config.has_key('rrdext')
    except AssertionError:
        raise RuntimeError("invalid module configuration")

    try:
        configure(config)
        assert DATADIR == config['datadir']
        assert infos.CONFIG == config['config']
        assert rrd_utils.DATADIR == config['datadir']
        assert rrd_utils.RRDEXT == config['rrdext']
    except AssertionError:
        raise RuntimeError("unable to configure module")


def configure(config):
    """Configure module

    :param dict config: Configuration informations
    """
    _log.debug("module config: {0}".format(config))
    globals().update({
        'DATADIR': config.get('datadir'),
        })
    infos.CONFIG = config.get('config')
    rrd_utils.DATADIR = config.get('datadir')
    rrd_utils.RRDEXT = config.get('rrdext')
    try:
        assert infos.CONFIG is not None
        assert DATADIR is not None
        assert rrd_utils.DATADIR is not None
        assert rrd_utils.RRDEXT is not None
    except AssertionError:
        raise RuntimeError("unconfigured module")


def handle_request(*args, **kwargs):
    """Main module entry point
    """

    _log.debug("handling new request")
    _log.debug("args: {0}".format(args))
    _log.debug("kwargs: {0}".format(kwargs))

    # Converting positionnal args to kwargs
    # First positionnal arg is always dispatch, skipping it
    args = list(args)
    args.pop(0)
    fields = ["method", "target", "datatype", "cf", "start", "stop", "tmpl"]
    positionnal_args = dict(zip(fields[:len(args)], args))

    # arguments agreggation
    arguments = kwargs
    arguments.update(positionnal_args)

    _log.debug("arguments: {0}".format(arguments))

    # Unknown method will raise exception handled by dispatcher
    try:
        # Load munin configuration for requests
        infos.load_munin_configs()
        return requests.KNOWN_METHODS[arguments['method']](arguments)
    except Exception as e:
        print(e.__repr__())