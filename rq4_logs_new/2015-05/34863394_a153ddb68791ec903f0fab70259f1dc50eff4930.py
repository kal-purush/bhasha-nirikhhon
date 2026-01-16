import unittest

import mock
from mock import mock_open
import libvirt

from libvirt_wrapper import LibvirtConn


class TestLibvirtConn(unittest.TestCase):
    @mock.patch('libvirt.open')
    def setUp(self, mock_lopen):
        mock_lopen.return_value = libvirt.virConnect()
        try:
            self.lvirt = LibvirtConn()
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())

    @mock.patch('__builtin__.open', mock_open())
    @mock.patch('libvirt.virConnect.defineXML')
    def test_create_ok(self, mock_lvrt):
        try:
            self.lvirt.create_vm("vm.xml")
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())
        except IOError as ioe:
            self.fail(ioe.strerror + ioe.filename)

    @mock.patch('__builtin__.open')
    def test_create_fail_file(self, mock_opn):
        mock_opn.side_effect = IOError('IO Mock Error')
        self.assertRaises(IOError, self.lvirt.create_vm, "vm.xml")

    @mock.patch('__builtin__.open', mock_open())
    @mock.patch('libvirt.virConnect.defineXML')
    def test_create_fail(self, mock_lvrt):
        mock_lvrt.side_effect = libvirt.libvirtError('Mock Libvirt Error')
        self.assertRaises(libvirt.libvirtError,
                          self.lvirt.create_vm, "vm.xml")

    @mock.patch('libvirt.virDomain.undefine')
    @mock.patch('libvirt.virDomain.destroy')
    @mock.patch('libvirt.virDomain.isActive')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_delete_ok(self, mock_lookup, mock_isact, mock_destr, mock_undef):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        mock_isact.return_value = 1
        try:
            self.lvirt.delete_vm("vm")
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())

    @mock.patch('libvirt.virConnect.lookupByName')
    def test_delete_fail_lookup(self, mock_lookup):
        mock_lookup.side_effect = libvirt.libvirtError("Mock Lvirt Error")
        self.assertRaises(libvirt.libvirtError, self.lvirt.delete_vm, "vm")

    @mock.patch('libvirt.virDomain.undefine')
    @mock.patch('libvirt.virDomain.destroy')
    @mock.patch('libvirt.virDomain.isActive')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_delete_fail_undef(self, mock_lookup, mock_isact,
                               mock_destr, mock_undef):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        mock_isact.return_value = 0
        mock_undef.side_effect = libvirt.libvirtError('Mock Lvirt Error')
        self.assertRaises(libvirt.libvirtError, self.lvirt.delete_vm, "vm")

    @mock.patch('libvirt.virDomain.create')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_poweron_ok(self, mock_lookup, mock_create):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        try:
            self.lvirt.power_on("vm")
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())

    @mock.patch('libvirt.virDomain.create')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_poweron_fail(self, mock_lookup, mock_create):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        mock_create.side_effect = libvirt.libvirtError('Mock Lvirt Error')
        self.assertRaises(libvirt.libvirtError, self.lvirt.power_on, "vm")

    @mock.patch('libvirt.virDomain.shutdown')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_poweroff_ok(self, mock_lookup, mock_shut):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        try:
            self.lvirt.power_off("vm")
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())

    @mock.patch('libvirt.virDomain.shutdown')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_poweroff_fail(self, mock_lookup, mock_shut):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        mock_shut.side_effect = libvirt.libvirtError('Mock Lvirt Error')
        self.assertRaises(libvirt.libvirtError, self.lvirt.power_off, "vm")

    @mock.patch('libvirt.virDomain.reboot')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_reboot_ok(self, mock_lookup, mock_reboot):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        try:
            self.lvirt.reboot("vm")
        except libvirt.libvirtError as le:
            self.fail(le.get_error_message())

    @mock.patch('libvirt.virDomain.reboot')
    @mock.patch('libvirt.virConnect.lookupByName')
    def test_reboot_fail(self, mock_lookup, mock_reboot):
        mock_lookup.return_value = libvirt.virDomain(self.lvirt.conn)
        mock_reboot.side_effect = libvirt.libvirtError('Mock Lvirt Error')
        self.assertRaises(libvirt.libvirtError, self.lvirt.reboot, "vm")


if __name__ == '__main__':
    unittest.main()