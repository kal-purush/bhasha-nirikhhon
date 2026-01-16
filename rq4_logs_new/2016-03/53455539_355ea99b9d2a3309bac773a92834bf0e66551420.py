# (c) 2016, Fran Fitzpatrick <francis.x.fitzpatrick@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import os
import shutil
import subprocess
import select
import fcntl
import getpass
import time

from ansible.compat.six import text_type, binary_type

import ansible.constants as C

from ansible.errors import AnsibleError, AnsibleFileNotFound
from ansible.plugins.connection import ConnectionBase
from ansible.utils.unicode import to_bytes, to_str

try:
    import boto3
except ImportError:
    raise AnsibleError("boto3 is not installed")

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()


class Connection(ConnectionBase):
    ''' AWS Run Command based connections '''

    _shell = 'sh'

    @property
    def transport(self):
        ''' used to identify this connection object '''
        return 'awsrun'

    def __init__(self, play_context, new_stdin, *args, **kwargs):
        super(Connection, self).__init__(play_context, new_stdin, *args, **kwargs)
        self._connect()

    def _connect(self):
        ''' connect to the local host; nothing to do here '''

        # TODO: Make this more ansible-like
        _AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
        _AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
        _AWS_REGION_NAME = os.environ['AWS_REGION_NAME']

        if not self._connected:
            display.vvv("Connecting to AWS SSM")
            display.vvv("AWS_ACCESS_KEY_ID: {}".format(_AWS_ACCESS_KEY_ID))
            display.vvv("AWS_REGION_NAME: {}".format(_AWS_REGION_NAME))

            self._session = boto3.Session(aws_access_key_id=_AWS_ACCESS_KEY_ID,
                                          aws_secret_access_key=_AWS_SECRET_ACCESS_KEY,
                                          region_name=_AWS_REGION_NAME)
            self._ssm = self._session.client('ssm')
            self._ec2 = self._session.resource('ec2')
            self._connected = True
            display.debug("Connected!")

        return self

    def _ip_to_instance_id(self, ip):
        # Not sure if I should be filtering for only running instances here...
        filt = [{'Name': 'instance-state-name', 'Values': ['running']},
                {'Name': 'ip-address', 'Values': [ip]}]
        instances = self._ec2.instances.filter(Filters=filt)
        instance = list(instances)[0]
        return instance.id

    def _exec_command(self, cmd, instance_id):
        # TODO: Windows, or linux? Hmmmmm
        display.vvv("Sending cmd to SSM Instance {}: {}".format(instance_id,
                                                                cmd))
        resp = self._ssm.send_command(InstanceIds=[instance_id],
                                      DocumentName='AWS-RunShellScript',
                                      TimeoutSeconds=60,
                                      Parameters={'commands': [cmd]})
        command_id = resp['Command']['CommandId']
        return command_id

    def _get_command_results(self, command_id):
        commands = self._ssm.list_command_invocations(CommandId=command_id,
                                                      Details=True)
        # What about timeouts?
        # What about other statuses other than success? Like an error?
        while commands['CommandInvocations'][0]['Status'] != 'Success':
            display.vvv("Command is not yet complete. Waiting a bit...")
            time.sleep(.25)
            commands = self._ssm.list_command_invocations(CommandId=command_id,
                                                          Details=True)

        display.vvv("Command is done!")
        details = commands['CommandInvocations'][0]['CommandPlugins'][0]
        result_code = details['ResponseCode']
        result_output = details['Output']
        display.vvv("Result Code: {}".format(result_code))
        display.vvv("Result Output: {}".format(result_output))
        # No stderr? Hmm.
        return (result_code, result_output)

    def exec_command(self, cmd, in_data=None, sudoable=True):
        ''' run a command on the local host '''
        super(Connection, self).exec_command(cmd, in_data=in_data, sudoable=sudoable)


        command_id = self._exec_command(cmd, self._instance_id)
        result = self._get_command_results(command_id)

        return (result[0], result[1], '')

    def put_file(self, in_path, out_path):
        super(Connection, self).put_file(in_path, out_path)
        display.vvv("PUT {} -> {}".format(in_path, out_path))

    def fetch_file(self, in_path, out_path):
        super(Connection, self).fetch_file(in_path, out_path)
        display.vvv("FETCH {} -> {}".format(in_path, out_path))

    def set_host_overrides(self, host):
        ip = '52.37.193.143'
        self._instance_id = self._ip_to_instance_id(ip)

    def close(self):
        ''' terminate the connection; nothing to do here '''
        self._client = None
        self._connected = False
        display.debug("The connection has been closed")