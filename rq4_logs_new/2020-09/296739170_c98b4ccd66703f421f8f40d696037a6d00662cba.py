# -*- coding: utf-8 -*-
import os
#  from configparser import SafeConfigParser
import sys
if sys.version_info < (3, 2):
    from ConfigParser import SafeConfigParser as ConfigParser
else:
    from configparser import ConfigParser


def get_config(section, env='dev'):
    config_filename = "settings.{env}.conf".format(env=env)
    parser = ConfigParser()
    filepath = os.sep.join(__file__.split(os.sep)[0:-1] + [config_filename])
    if os.path.isfile(filepath):
        parser.read(filepath)
    else:
        raise Exception('{0} is not a file'.format(filepath))

    # get config section
    conf = {}
    if parser.has_section(section):
        params = parser.items(section)
        for key, val in params:
            conf[key] = val
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filepath))

    return conf