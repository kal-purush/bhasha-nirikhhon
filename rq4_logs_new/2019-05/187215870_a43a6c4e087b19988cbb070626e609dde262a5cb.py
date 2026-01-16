#!/usr/bin/python

DOCUMENTATION = '''
---
module: firewalld_zone

short_description: Manage firewalld zones

description:
- Manage firewalld zones

options:
  firewalld_cmd:
    description:
    - Firewalld command if not "firewall-cmd"
    default: firewalld-cmd
  state:
    description:
    - Desired zone state
    default: present
    choices: [ absent, present ]
  zone:
    description:
    - Firewalld zone name
    required: true
requirements:
- 'firewalld >= 0.6.2'
author: Johnathan Kupferer <jkupfere@redhat.com>
'''

EXAMPLES = '''
- name: Define custom firewalld zone
  firewalld_zone:
    zone: clients

- name: Remove custom firewalld zone
  firewalld_zone:
    zone: database
    state: absent
'''

RETURN = '''
zone:
  description: Zone nome
  type: string
state:
  description: Zone state
  type: string
'''

import traceback

from ansible.module_utils.basic import AnsibleModule

class FirewalldZone:
    def __init__(self, module):
        self.module = module
        self.changed = False
        self.state = module.params['state']
        self.firewall_cmd = module.params['firewall_cmd']
        self.zone = module.params['zone']

    def run_firewall_cmd(self, args, **kwargs):
        check_rc = True
        if 'check_rc' in kwargs:
            check_rc = kwargs['check_rc']
        kwargs['check_rc'] = False

        (return_code, stdout, stderr) = self.module.run_command([self.firewall_cmd] + args, **kwargs)

        if return_code != 0 and check_rc:
            self.module.fail_json(cmd=args, return_code=return_code, stdout=stdout, stderr=stderr, msg=stderr)

        return (return_code, stdout, stderr)

    def create(self):
        (return_code, stdout, stderr) = self.run_firewall_cmd(
            [
                '--permanent',
                '--path-zone=' + self.zone
            ],
            check_rc=False
        )

        # Zone already exists, nothing to create
        if return_code == 0:
            return

        # Indicate that a change is required
        self.changed = True

        # Return if check mode
        if self.module.check_mode:
            return

        (return_code, stdout, stderr) = self.run_firewall_cmd(
            [
                '--permanent',
                '--new-zone=' + self.zone
            ],
            check_rc=True
        )

    def delete(self):
        (return_code, stdout, stderr) = self.run_firewall_cmd(
            [
                '--permanent',
                '--path-zone=' + self.zone
            ],
            check_rc=False
        )

        # Zone not found, nothing to delete
        if return_code != 0:
            return

        # Indicate that a change is required
        self.changed = True

        # Return if check mode
        if self.module.check_mode:
            return

        (return_code, stdout, stderr) = self.run_firewall_cmd(
            [
                '--permanent',
                '--delete-zone=' + self.zone
            ],
            check_rc=True
        )

def run_module():
    module = AnsibleModule(
        argument_spec={
            'firewall_cmd': {
                'type': 'str',
                'required': False,
                'default': 'firewall-cmd'
            },
            'state': {
                'choices': ['present', 'absent'],
                'required': False,
                'default': 'present'
            },
            'zone': {
                'type': 'str',
                'required': True
            }
        },
        supports_check_mode=True
    )

    firewalld_zone = FirewalldZone(module)

    try:
        if firewalld_zone.state == 'present':
            firewalld_zone.create()
        elif firewalld_zone.state == 'absent':
            firewalld_zone.delete()
        else:
            raise Exception('Invalid state: "{}"'.format(firewalld_zone.state))

        module.exit_json(
            state=firewalld_zone.state,
            zone=firewalld_zone.zone,
            changed=firewalld_zone.changed
        )
    except Exception as e:
        module.fail_json(
            msg=str(e),
            state=firewalld_zone.state,
            traceback=traceback.format_exc().split('\n')
        )

def main():
    run_module()

if __name__ == "__main__":
    main()