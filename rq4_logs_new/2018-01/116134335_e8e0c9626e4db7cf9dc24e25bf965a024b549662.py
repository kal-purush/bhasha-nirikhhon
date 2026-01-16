#!/usr/bin/env python

"""
Python program for cloning a vm on an ESX / vCenter host
"""

from __future__ import print_function
from ansible.module_utils.basic import AnsibleModule
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import atexit


def get_obj(content, vimtype, name):
    """
    Return an object (vm, for example) by name, if name is None the
    first found object is returned
    """
    obj = None
    container = content.viewManager.CreateContainerView(
        content.rootFolder, vimtype, True)
    for c in container.view:
        if name:
            if c.name == name:
                obj = c
                break
        else:
            obj = c
            break

    return obj


def wait_for_task(task):
    """ wait for a vCenter task to finish """
    while True:
        if task.info.state == 'success':
            return {'task_done': True, 'error': ''}

        if task.info.state == 'error':
            return {'task_done': False, 'error': 'There was an error when creating the VM: %s' % task.info.error.msg}


def clone_vm(content, source_vm, dest_vm_name, custom_specs, si, vmware_hosts_cluster):
    """
    Clone a VM from another vm, specifying which source_vm to use.
    "custom_specs" refers to a template with predefined hardware specs we want to use for the new vm
    "si" is the connection handler to Vmware
    """

    # get info about the path/folder of the current vm
    destfolder = source_vm.parent

    # get info about the datastore of the current vm
    datastore = get_obj(content, [vim.Datastore], source_vm.datastore[0].info.name)

    # get info about the host and cluster of the current vm
    cluster = get_obj(content, [vim.ClusterComputeResource], vmware_hosts_cluster)
    resource_pool = cluster.resourcePool

    # This gets the MOID of the Guest Customization Spec that is saved in the vCenter DB
    guest_customization_spec = si.content.customizationSpecManager.GetCustomizationSpec(name=custom_specs)

    # now we have all the info of the current vm and we'll go and clone it
    relospec = vim.vm.RelocateSpec()
    relospec.datastore = datastore
    relospec.pool = resource_pool

    clonespec = vim.vm.CloneSpec()
    clonespec.location = relospec
    clonespec.powerOn = True
    clonespec.customization = guest_customization_spec.spec

    # Sending order to clone to VCenter...
    task = source_vm.Clone(folder=destfolder, name=dest_vm_name, spec=clonespec)
    # Waiting for VM to be cloned...
    return wait_for_task(task)


def main():
    """
    Simple program for cloning a virtual machine.
    """

    # first we get the params from Ansible playbook
    fields = {
        "vmware_host": {"required": True, "type": "str"},
        "vmware_user": {"required": True, "type": "str"},
        "vmware_pwd": {"required": True, "type": "str"},
        "vmware_port": {"required": True, "type": "str"},
        "vmware_hosts_cluster": {"required": True, "type": "str"},
        "vmware_source_vm": {"required": True, "type": "str"},
        "vmware_dest_vm": {"required": True, "type": "str"},
    }
    ansiblemodule = AnsibleModule(argument_spec=fields)

    context = None
    if hasattr(ssl, '_create_unverified_context'):
        context = ssl._create_unverified_context()

    # no we connect to Vmware with the info we received
    si = SmartConnect(host=ansiblemodule.params['vmware_host'],
                      user=ansiblemodule.params['vmware_user'],
                      pwd=ansiblemodule.params['vmware_pwd'],
                      port=int(ansiblemodule.params['vmware_port']),
                      sslContext=context)

    if not si:
        ansiblemodule.fail_json(msg="Could not connect to the specified host using specified username and password")

    atexit.register(Disconnect, si)

    content = si.RetrieveContent()

    # now we get info of the current vm we want to clone from
    source_vm = get_obj(content, [vim.VirtualMachine], ansiblemodule.params['vmware_source_vm'])

    # and finally we create the vm if we found the info for the current one
    if source_vm:
        result = clone_vm(content=content, source_vm=source_vm, dest_vm_name=ansiblemodule.params['vmware_dest_vm'],
                          custom_specs=ansiblemodule.params['vmware_dest_vm'], si=si,
                          vmware_hosts_cluster=ansiblemodule.params['vmware_hosts_cluster'])
        if result['task_done']:
            ansiblemodule.exit_json(changed=True, msg="The new VM has been created")
        else:
            ansiblemodule.fail_json(msg=result['error'])
    else:
        ansiblemodule.fail_json(msg="Source VM not found!")

    Disconnect(si)


# Start program
if __name__ == "__main__":
    main()