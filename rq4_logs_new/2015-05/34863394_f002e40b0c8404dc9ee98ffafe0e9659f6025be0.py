import libvirt
import sys
import argparse

class LibvirtConn:
    (ECONN, EFIND, ECREATE, EDELETE, ESTART, ESTOP, EREBOOT) = (1,2,3,4,5,6,7)
    def __init__(self):
        try:
            self.conn = libvirt.open("qemu:///system")
        except libvirt.libvirtError:
            raise SystemExit(self.ECONN)

    def find_vm_dec(func):
        def closure(self, name, *args, **kwargs):
                try:
                    vm = self.conn.lookupByName(name)
                except libvirt.libvirtError:
                    raise SystemExit(self.EFIND)
                return func(self, name, vm, *args, **kwargs)
        return closure

    def create_vm(self, xml_file):
        with open(xml_file) as f:
            xml = f.read()
        try:
            self.conn.defineXML(xml)
        except libvirt.libvirtError:
            raise SystemExit(self.ECREATE)

    @find_vm_dec
    def delete_vm(self, name, vm):
        try:
            if vm.isActive():
                vm.destroy()
            vm.undefine()
        except libvirt.libvirtError:
           raise SystemExit(self.EDELETE)

    @find_vm_dec
    def power_on(self, name, vm):
        try:
            vm.create()
        except libvirt.libvirtError:
            raise SystemExit(self.ESTART)

    @find_vm_dec
    def power_off(self, name, vm):
        try:
            vm.shutdown()
        except libvirt.libvirtError:
            raise SystemExit(self.ESTOP)

    @find_vm_dec
    def reboot(self, name, vm):
        try:
            vm.reboot()
        except libvirt.libvirtError:
            raise SystemExit(self.EREBOOT)


parser = argparse.ArgumentParser(description="Pythonic wrapper for libvirt. "
    "Author: Ruslan Aliev")

parser.add_argument('--create', dest='xml_file', metavar='VM_NAME',
    help='Create VM defined in XML_FILE')
parser.add_argument('--delete', dest='name_del', metavar='VM_NAME',
    help='Delete VM')
parser.add_argument('--power-on', dest='name_on', metavar='VM_NAME',
    help='Power ON VM')
parser.add_argument('--power-off', dest='name_off', metavar='VM_NAME',
    help='Power OFF VM')
parser.add_argument('--reboot', dest='name_reboot', metavar='VM_NAME',
    help='Reboot VM')

args = parser.parse_args()

lvirt = LibvirtConn()

if args.xml_file:
    lvirt.create_vm(args.xml_file)
elif args.name_del:
    lvirt.delete_vm(args.name_del)
elif args.name_on:
    lvirt.power_on(args.name_on)
elif args.name_off:
    lvirt.power_off(args.name_off)
elif args.name_reboot:
    lvirt.reboot(args.name_reboot)
else:
    print "No options specified. Use -h for help"
