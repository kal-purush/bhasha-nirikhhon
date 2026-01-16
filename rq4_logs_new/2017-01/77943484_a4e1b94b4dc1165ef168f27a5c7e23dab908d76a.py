#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# add-hosts.py, ABr
# Add a host to backing ansible file

import argparse
import json
import re
from subprocess import call
import sys
import types

__version__ = '0.1'

class DoRun(object):
  def __init__(
    self,
    path,
    hostname_public,
    hostname_internal,
    groups,
    opt_arg_keys,
    opt_arg_values
  ):
    # save vars
    self._path = path
    self._hostname_public = hostname_public
    self._hostname_internal = hostname_internal
    self._groups = groups
    self._opt_arg_keys = opt_arg_keys
    self._opt_arg_values = opt_arg_values

    # exec
    self.run()

  # parse configuration file
  def readConfig(self, lines):
    # result data
    configData = {}

    # work vars
    curgroup = ''
    line_no = 0
    curgroup_obj = None

    # process
    for line in lines:
      line_no += 1
      line = line.rstrip()
      if re.search('^\s*$', line): continue
      if re.search('^\s*#', line): continue

      # check for new group
      if re.search('^\s*\[', line):
        group_match = re.match('^\s*\[([^\]]+)\]\s*$', line);
        if not group_match: raise ValueError("Line %d: Invalid group" % (line_no))
        curgroup = group_match.group(1)
        continue

      # data expected. require a group.
      if not curgroup: raise ValueError("Line %d: No group specified" % (line_no))

      # must be data
      data_elements = line.split('\t')
      hostname_public = data_elements[0]

      # get the group reference (auto-create)
      if curgroup in configData:
        curgroup_obj = configData[curgroup]
      else:
        curgroup_obj = {}
        configData[curgroup] = curgroup_obj

      # add hostname
      curhost_obj = {}
      curgroup_obj[hostname_public] = curhost_obj

      # read remainder of items
      key_value_idx = 0
      for key_value in data_elements[1:]:
        key_value_idx += 1

        # must be key=value pair
        key_value_match = re.match('^\s*([^=]+)=(.*)', key_value)
        if not key_value_match: raise ValueError("Line %d: Invalid key value pair #%d" % (line_no, key_value_idx))

        # store
        key = key_value_match.group(1)
        value = key_value_match.group(2)
        curhost_obj[key] = value

    # final result
    return configData

  # update configuration file
  def writeConfig(self, configData, fp):
    # write each element

    for group in configData:
      # group entry
      fp.write('[%s]\n' % (group))

      # now hosts defined in that group
      curgroup_obj = configData[group]
      for host in curgroup_obj:
        # write hostname
        fp.write('%s' % (host))

        # now options
        curhost_obj = curgroup_obj[host]
        for key in curhost_obj:
          value = curhost_obj[key]
          fp.write('\t%s=%s' % (key, value))

        # close out the line
        fp.write('\n')

  def run(self):
    # get the optional arguments
    if self._opt_arg_keys:
      opt_arg_keys = list(self._opt_arg_keys)
    else:
      opt_arg_keys = []

    if self._opt_arg_values:
      opt_arg_values = list(self._opt_arg_values)
    else:
      opt_arg_values = []

    if len(opt_arg_keys) != len(opt_arg_values):
      raise ValueError("opt_arg_keys != opt_arg_values")

    # read the existing data
    with open(self._path, 'r') as configfile:
      configData = self.readConfig(configfile)

    # check for the host in every entry and *delete*
    curGroupObj = None
    curHostObj = None
    for group in configData:
      curGroupObj = configData[group]
      if self._hostname_public in curGroupObj:
        del curGroupObj[self._hostname_public]

    # add the host to every specified group
    for group in self._groups:
      # auto-create the group
      if group in configData:
        curGroupObj = configData[group]
      else:
        curGroupObj = {}
        configData[group] = curGroupObj

      # store data
      curHostObj = {}
      curHostObj['hostnameinternal'] = self._hostname_internal
      curGroupObj[self._hostname_public] = curHostObj

      # store optional arguments
      for i, item in enumerate(opt_arg_keys):
        curHostObj[item] = opt_arg_values[i]

    # now we write the config
    with open(self._path, 'w') as configfile:    # save
      self.writeConfig(configData, configfile)

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-V', '--version', action='version', version=__version__)
  parser.add_argument('--path', help='Path to the INI file', required=True)
  parser.add_argument('--hostname-public', help='Hostname to add (public)', required=True)
  parser.add_argument('--hostname-internal', help='Hostname to add (internal)', required=True)
  parser.add_argument('--groups', help='Groups for this host', nargs='+', required=True)
  parser.add_argument('--opt-arg-keys', help='Optional argument keys', nargs='*', required=False)
  parser.add_argument('--opt-arg-values', help='Optional argument values', nargs='*', required=False)
  args = parser.parse_args()
  DoRun(
    path = args.path,
    hostname_public = args.hostname_public,
    hostname_internal = args.hostname_internal,
    groups = args.groups,
    opt_arg_keys = args.opt_arg_keys,
    opt_arg_values = args.opt_arg_values
  )

if __name__ == '__main__':
  main()
