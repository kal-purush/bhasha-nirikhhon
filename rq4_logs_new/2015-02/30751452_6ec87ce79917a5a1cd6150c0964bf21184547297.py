#!/usr/bin/env python

DOCUMENTATION = '''
---
module: vbox_instance
short_description: Manages a VirtualBox host
description:
   - Manage and provision VirtualBox hosts by cloning.
options:
  vboxmanage:
    description:
      - path to VirtualBox's vboxmanage command
    required: false
    default: '/Applications/VMware Fusion.app/Contents/Library/vmrun'
  source_image:
    description:
      - name of the source VM to clone from
    required: true
  target_image:
    description:
      - name for the target VM
    required: true
    '''

EXAMPLES = '''
# Create a clone image called web01.localdomain from a source image called base-image-centos6
- vbox_instance: source_image='base-image-centos6' target_image='web01.localdomain'
'''

class VBox():
    def __init__(self, module, vboxmanage, source_image, target_image, state):
        self.module = module
        self.vboxmanage = vboxmanage
        self.source_image = source_image
        self.target_image = target_image
        self.state = state

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
        p = self.exec_command(self.escape_spaces(self.vboxmanage) + ' list runningvms')
        if p.returncode != 0:
            self.module.fail_json(msg='Error determining if target instance is running')
        for stdoutline in p.stdout.readlines():
            if re.match('^"' + self.target_image + '"', stdoutline):
                return True
        return False

    def is_created(self, vm):
        p = self.exec_command(self.escape_spaces(self.vboxmanage) + ' list vms')
        if p.returncode != 0:
            self.module.fail_json(msg='Error determining if instance is created')
        for stdoutline in p.stdout.readlines():
            if re.match('^"' + vm + '"', stdoutline):
                return True
        return False


    def get_snapshot(self):
        p = self.exec_command(self.escape_spaces(self.vboxmanage) + ' snapshot ' + self.source_image + ' list')
        if p.returncode != 0:
            self.module.fail_json(msg='Error determining linked base snapshot')
        for stdoutline in p.stdout.readlines():
            s = re.search('Name: Linked Base for ' + self.source_image + '.*\(.*UUID: (.+)\)', stdoutline)
            if s:
                return s.groups()[0]
        return False

    def clone_vm(self):
        snapshotid = self.get_snapshot()
        if snapshotid == None:
            self.module.fail_json(msg='Failed to get snapshot id')
        p = self.exec_command(self.vboxmanage + ' clonevm ' + self.source_image +
                              ' --options link --name ' + self.target_image +
                              '  --snapshot ' + snapshotid + ' --register')
        if p.returncode != 0:
            raise Exception('Oops!')
        return True

    def start_vm(self):
        if not self.is_created(self.target_image):
            self.clone_vm()
        p = self.exec_command(self.vboxmanage + ' startvm ' + self.target_image + ' --type gui')
        if p.returncode != 0:
            self.module.fail_json(msg='Error trying to start VM')

    def stop_vm(self):
        pass


def main():
    module = AnsibleModule(
        argument_spec=dict(
            vboxmanage=dict(default='/usr/bin/VBoxManage'),
            source_image=dict(required=True),
            target_image=dict(required=True),
            state=dict(default='running'),
        )
    )

    vboxmanage = module.params["vboxmanage"]
    source_image = module.params["source_image"]
    target_image = module.params["target_image"]
    state = module.params["state"]

    v = VBox(module, vboxmanage, source_image, target_image, state)

    if state == 'running':
        if v.is_running:
            msg = 'target instance: ' + target_image + ' running'
            module.exit_json(changed=False, msg=msg)
        else:
            v.start_vm()


from ansible.module_utils.basic import *

if __name__ == '__main__':
    main()


