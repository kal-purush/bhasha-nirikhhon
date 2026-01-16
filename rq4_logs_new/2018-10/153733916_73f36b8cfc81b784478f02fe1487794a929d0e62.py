
from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

try:
    from pyVmomi import vim
except ImportError:
    pass

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_text
from ansible.module_utils.vmware import PyVmomi, vmware_argument_spec
from glob import glob
from os.path import basename, dirname

class PyVmomiHelper(PyVmomi):
    def __init__(self, module):
        super(PyVmomiHelper, self).__init__(module)
    def gather_disk_facts(self, vm_obj):
        """
        Function to gather facts about VM's disks
        Args:
            vm_obj: Managed object of virtual machine

        Returns: A list of dict containing disks information

        """
        disks_facts = dict()
        if vm_obj is None:
            return disks_facts

        disk_index = 0
        for disk in vm_obj.config.hardware.device:
            if isinstance(disk, vim.vm.device.VirtualDisk):
                disks_facts[disk_index] = dict(
                    label=disk.deviceInfo.label,
                    summary=disk.deviceInfo.summary,
                    backing_filename=disk.backing.fileName,
                    backing_datastore=disk.backing.datastore.name,
                    backing_disk_mode=disk.backing.diskMode,
                    capacity_in_kb=disk.capacityInKB,
                )
                disk_index += 1
        count_disk = len(disks_facts)
        disks_facts.update({'No_of_Disk_present':count_disk}) 
        return disks_facts
     
   
def main():
    argument_spec = vmware_argument_spec()
    argument_spec.update(
        name=dict(type='str'),
        name_match=dict(type='str', choices=['first', 'last'], default='first'),
        uuid=dict(type='str'),
        folder=dict(type='str', default='/vm'),
        datacenter=dict(type='str', required=False),
    )
    module = AnsibleModule(argument_spec=argument_spec,
                           required_one_of=[['name', 'uuid']])


    pyv = PyVmomiHelper(module)
    # Check if the VM exists before continuing
    vm = pyv.get_vm()

    # VM already exists
    if vm:
        try:
            module.exit_json(guest_disk_facts=pyv.gather_disk_facts(vm))
        except Exception as exc:
            module.fail_json(msg="Failed to gather facts with exception : %s" % to_text(exc))
   
    else:
        module.fail_json(msg="Unable to gather facts for non-existing VM %s" % module.params.get('uuid') or module.params.get('name'))


if __name__ == '__main__':
    main()
