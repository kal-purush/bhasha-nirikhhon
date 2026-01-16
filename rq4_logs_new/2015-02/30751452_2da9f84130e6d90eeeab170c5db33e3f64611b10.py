#!/usr/bin/env python

DOCUMENTATION = '''
---
module: fusion_instance
short_description: Manages a VMWare Fusion host
description:
   - Manage and provision VMWare Fusion hosts by cloning.
options:
  vmrunexe:
    description:
      - path to VMWare Fusion's vmrun command
    required: false
    default: '/Applications/VMware Fusion.app/Contents/Library/vmrun'
  vmbasedir:
    description:
      - path to virtual machine folder
    required: fals
    default: '~/Documents/Virtual Machines.localized'
  source_image:
    description:
      - name of the source VM to clone from
    required: true
  target_image:
    description:
      - name for the target VM
    required: true
  memsize:
    description:
      - amount of physical memory (in MB) to allocate to the VM
    required: false
    default: '512'
  clone_type:
    description:
      - the type of clone to perform
    required: false
    default: 'linked'
    choices: ['linked','full']
  headless:
    description:
      - with or without gui
    required: false
    default: 'no'
    choices: ['yes','no']
  state:
    description:
      - create or terminate instances
    required: false
    default: 'running'
    choices: ['running', 'absent']
'''

EXAMPLES = '''
# Create a clone image called web01.localdomain from a source image called base-image-centos6
- fusion_instance: source_image='base-image-centos6' target_image='web01.localdomain'
'''


class Fusion():
    def __init__(self, source_image, target_image, memsize, clone_type, headless, vmrunexe, vmbasedir):
        self.vmrunexe = vmrunexe
        self.vmbasedir = vmbasedir
        self.source_image = source_image
        self.target_image = target_image
        self.memsize = memsize
        self.clone_type = clone_type
        self.headless = headless
        self.source_vmx = os.path.join(self.vmbasedir, self.source_image + '.vmwarevm', self.source_image + '.vmx')
        self.target_vmx = os.path.join(self.vmbasedir, self.target_image + '.vmwarevm', self.target_image + '.vmx')

        if not os.path.isfile(self.vmrunexe):
            raise Exception('Cannot find vmrunexe: ' + self.vmrunexe)
        if not os.path.isdir(self.vmbasedir):
            raise Exception('Cannot find vmbasedir: ' + self.vmbasedir)

    @staticmethod
    def escape_spaces(s):
        return s.replace(' ', '\ ')

    @staticmethod
    def exec_command(command):
        p = subprocess.Popen(command, shell=True, executable='/bin/bash',
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.wait()
        return p

    @property
    def is_running(self):
        p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' list')
        if p.returncode != 0:
            return False
        stdout = p.stdout.readlines()
        for stdoutline in stdout:
            if self.target_vmx == stdoutline.replace('\n', ''):
                return True
        return False

    @property
    def ipaddress(self):
        maxtries = 60
        tries = 0
        while tries < maxtries:
            p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' getGuestIPAddress ' +
                                  self.escape_spaces(self.target_vmx))
            if p.returncode != 0:
                tries += 1
                sleep(1)
            else:
                ipaddress = p.stdout.read().replace('\n', '')
                return ipaddress
        return False

    def clone_vm(self):
        if not os.path.isfile(self.source_vmx):
            raise Exception('Unable to find base image')
        if not os.path.isfile(self.target_vmx):
            p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' clone ' +
                                  self.escape_spaces(self.source_vmx) + ' ' +
                                  self.escape_spaces(self.target_vmx) + ' ' + self.clone_type)
            if p.returncode != 0:
                raise Exception('Ooops!')
            # update the vmx config...
            new_config = []
            fh = open(self.target_vmx, 'r')
            current_config = fh.readlines()
            fh.close()
            for config_line in current_config:
                if re.match('^displayname = "', config_line):
                    config_line = 'displayname = "' + self.target_image + '"\n'
                if re.match('^displayName = "', config_line):
                    config_line = 'displayName = "' + self.target_image + '"\n'
                if re.match('^memsize = "', config_line):
                    config_line = 'memsize = "' + self.memsize + '"\n'
                new_config.append(config_line)
            fh = open(self.target_vmx, 'w')
            fh.write(''.join(new_config))
            fh.close()

    def start_vm(self):
        if not os.path.isfile(self.target_vmx):
            self.clone_vm()
        if self.headless == 'yes':
            guiparam = 'nogui'
        else:
            guiparam = 'gui'
        p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' start ' +
                              self.escape_spaces(self.target_vmx) + ' ' + guiparam)
        if p.returncode != 0:
            return False
        return True

    def stop_vm(self):
        if not os.path.isfile(self.target_vmx):
            raise Exception('Unable to find target vmx file: ' + self.target_vmx)
        if not self.is_running:
            raise Exception('Image ' + self.target_image + ' not running')
        print 'stopping ' + self.target_image
        p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' stop ' +
                              self.escape_spaces(self.target_vmx))
        if p.returncode != 0:
            raise Exception('Oops!')
        if self.is_running:
            raise Exception('Failed to stop ' + self.target_image)

    def delete_vm(self):
        if not os.path.isfile(self.target_vmx):
            raise Exception('Unable to find image')
        if self.is_running:
            self.stop_vm()
        p = self.exec_command(self.escape_spaces(self.vmrunexe) + ' deleteVM ' +
                              self.escape_spaces(self.target_vmx))
        if p.returncode != 0:
            raise Exception('Oops!')
        print 'deleted ' + self.target_image


def main():
    module = AnsibleModule(
        argument_spec=dict(
            vmrunexe=dict(default='/Applications/VMware Fusion.app/Contents/Library/vmrun'),
            vmbasedir=dict(default=os.path.expanduser("~") + '/Documents/Virtual Machines.localized'),
            source_image=dict(required=True),
            target_image=dict(required=True),
            memsize=dict(default='512'),
            clone_type=dict(default='linked'),
            headless=dict(default='no'),
            state=dict(default='running'),
        )
    )

    vmrunexe = module.params["vmrunexe"]
    vmbasedir = module.params["vmbasedir"]
    source_image = module.params["source_image"]
    target_image = module.params["target_image"]
    memsize = module.params["memsize"]
    clone_type = module.params["clone_type"]
    headless = module.params["headless"]
    state = module.params["state"]

    f = Fusion(source_image=source_image, target_image=target_image, memsize=memsize,
               clone_type=clone_type, headless=headless, vmbasedir=vmbasedir, vmrunexe=vmrunexe)

    if state == 'running':
        if f.is_running:
            msg = 'instance: ' + target_image + ' running'
            module.exit_json(changed=False, msg=msg, ansible_facts=dict(ipaddress=f.ipaddress))
        else:
            if f.start_vm():
                if f.is_running:
                    msg = 'instance: ' + target_image + ' running'
                    module.exit_json(changed=True, msg=msg, ansible_facts=dict(ipaddress=f.ipaddress))
    elif state == 'absent':
        if os.path.isfile(f.target_vmx):
            f.delete_vm()
            msg = 'instance: ' + target_image + ' absent'
            module.exit_json(changed=True, msg=msg)
        else:
            msg = 'instance: ' + target_image + ' absent'
            module.exit_json(changed=False, msg=msg)

from ansible.module_utils.basic import *
import os
import subprocess
import re
from time import sleep

if __name__ == '__main__':
    main()