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

import ConfigParser
import os

EXIT_CFG_SYNTAX=1

CONF_NAME='dispytch.conf'

config_dict = {}

def get_file_path():
    """
    Get file path

    :return: path
    :rtype: string
    """
    return os.path.sep.join([os.path.dirname(__file__), CONF_NAME])


def load_config_file():
    """
    Load config file

    Configuration file dispytch.conf should be in dispytch directory
    
    :return: ConfigParser
    """
    config_file = get_file_path()
    print(config_file)

    conf_parser = ConfigParser.ConfigParser()

    if not os.path.isfile(config_file) or not os.access(config_file, os.R_OK):
        print('File {0} not found or not readable'.format(config_file))
        exit(EXIT_CFG_SYNTAX)

    try:
        conf_parser.read(config_file)
    except ConfigParser.ParsingError as e:
        print(e.message)
        exit(EXIT_CFG_SYNTAX)

    return conf_parser


def parse_conf():
    """
    Parse configuratiguration file and translate it as structure

    :return: Configuration
    :rtype: dict
    """

    conf_parser = load_config_file()

    for section in conf_parser.sections():
        config_dict.update({section : {}})
        for option in conf_parser.options(section):
            value = conf_parser.get(section, option)
            config_dict[section].update({option : value})

    return config_dict

def get_section(section):
    """
    Get options of specific section

    :param section string: name of the section
    
    :return: options
    :rtype: dict
    """
    
    return config_dict.get(section) 