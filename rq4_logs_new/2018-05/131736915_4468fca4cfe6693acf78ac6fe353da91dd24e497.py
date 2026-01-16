from __future__ import print_function
import getpass
import inspect
import os
import psutil
import re
import shutil
import subprocess
import sys
import time

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

from ..configuration import IrodsConfig
from ..controller import IrodsController
from ..core_file import temporary_core_file, CoreFile
from .. import test
from . import settings
from .. import lib
from .resource_suite import ResourceSuite, ResourceBase
from .test_chunkydevtest import ChunkyDevTest
from . import session
from .rule_texts_for_tests import rule_texts

def statvfs_path_or_parent(path):
    while not os.path.exists(path):
        path = os.path.dirname(path)
    return os.statvfs(path)


class Test_Resource_Unixfilesystem(ResourceSuite, ChunkyDevTest, unittest.TestCase):
    plugin_name = IrodsConfig().default_rule_engine_plugin
    class_name = 'Test_Resource_Unixfilesystem'

    def setUp(self):
        hostname = lib.get_hostname()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin modresc demoResc name origResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
            #this test assumes that the iRODS server has a mount point /mnt/nfs that points to the stornext share.
            admin_session.assert_icommand("iadmin mkresc demoResc 'unixfilesystem' " + hostname + ":/mnt/nfs/demoRescVault", 'STDOUT_SINGLELINE', 'unixfilesystem')
        super(Test_Resource_Unixfilesystem, self).setUp()

    def tearDown(self):
        super(Test_Resource_Unixfilesystem, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand("iadmin rmresc demoResc")
            admin_session.assert_icommand("iadmin modresc origResc name demoResc", 'STDOUT_SINGLELINE', 'rename', input='yes\n')
        shutil.rmtree(IrodsConfig().irods_directory + "/demoRescVault", ignore_errors=True)

    def test_unix_filesystem_free_space__3306(self):
        filename = 'test_unix_filesystem_free_space__3306.txt'
        filesize = 3000000
        lib.make_file(filename, filesize)

        free_space = 10000000
        self.admin.assert_icommand('iadmin modresc demoResc freespace {0}'.format(free_space))

        # free_space already below threshold - should NOT accept new file
        minimum = free_space + 10
        self.admin.assert_icommand('iadmin modresc demoResc context minimum_free_space_for_create_in_bytes={0}'.format(minimum))
        self.admin.assert_icommand('iput ' + filename + ' file1', 'STDERR_SINGLELINE', 'USER_FILE_TOO_LARGE')

        # free_space will be below threshold if replica is created - should NOT accept new file
        minimum = free_space - filesize/2
        self.admin.assert_icommand('iadmin modresc demoResc context minimum_free_space_for_create_in_bytes={0}'.format(minimum))
        self.admin.assert_icommand('iput ' + filename + ' file2', 'STDERR_SINGLELINE', 'USER_FILE_TOO_LARGE')

        # after replica creation, free_space will still be greater than minimum - should accept new file
        minimum = free_space - filesize*2
        self.admin.assert_icommand('iadmin modresc demoResc context minimum_free_space_for_create_in_bytes={0}'.format(minimum))
        self.admin.assert_icommand('iput ' + filename + ' file3')

    def test_msi_update_unixfilesystem_resource_free_space_and_acPostProcForParallelTransferReceived(self):
        filename = 'test_msi_update_unixfilesystem_resource_free_space_and_acPostProcForParallelTransferReceived'
        filepath = lib.make_file(filename, 50000000)

        # make sure the physical path exists
        lib.make_dir_p(self.admin.get_vault_path('demoResc'))

        with temporary_core_file() as core:
            time.sleep(1)  # remove once file hash fix is committed #2279
            core.add_rule(rule_texts[self.plugin_name][self.class_name][inspect.currentframe().f_code.co_name])
            time.sleep(1)  # remove once file hash fix is committed #2279

            self.user0.assert_icommand(['iput', filename])
            free_space = psutil.disk_usage(self.admin.get_vault_path('demoResc')).free

        ilsresc_output = self.admin.run_icommand(['ilsresc', '-l', 'demoResc'])[0]
        for l in ilsresc_output.splitlines():
            if l.startswith('free space:'):
                ilsresc_freespace = int(l.rpartition(':')[2])
                break
        else:
            assert False, '"free space:" not found in ilsresc output:\n' + ilsresc_output
        assert abs(free_space - ilsresc_freespace) < 4096*10, 'free_space {0}, ilsresc free space {1}'.format(free_space, ilsresc_freespace)
        os.unlink(filename)

    def test_key_value_passthru(self):
        env = os.environ.copy()
        env['spLogLevel'] = '11'
        IrodsController(IrodsConfig(injected_environment=env)).restart()

        lib.make_file('file.txt', 15)
        initial_log_size = lib.get_file_size_by_path(IrodsConfig().server_log_path)
        self.user0.assert_icommand('iput --kv_pass="put_key=val1" file.txt')
        assert lib.count_occurrences_of_string_in_log(IrodsConfig().server_log_path, 'key [put_key] - value [val1]', start_index=initial_log_size) in [1, 2]  # double print if collection missing

        initial_log_size = lib.get_file_size_by_path(IrodsConfig().server_log_path)
        self.user0.assert_icommand('iget -f --kv_pass="get_key=val3" file.txt other.txt')
        assert lib.count_occurrences_of_string_in_log(IrodsConfig().server_log_path, 'key [get_key] - value [val3]', start_index=initial_log_size) in [1, 2]  # double print if collection missing
        IrodsController().restart()
        if os.path.exists('file.txt'):
            os.unlink('file.txt')
        if os.path.exists('other.txt'):
            os.unlink('other.txt')

    @unittest.skipIf(test.settings.RUN_IN_TOPOLOGY, "Skip for Topology Testing: Checks local file")
    def test_ifsck__2650(self):
        # local setup
        filename = 'fsckfile.txt'
        filepath = lib.create_local_testfile(filename)
        orig_digest = lib.file_digest(filepath, 'sha256', encoding='base64')
        long_collection_name = '255_byte_directory_name_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12_abcdefghijklmnopqrstuvwxyz12'
        self.admin.assert_icommand("imkdir " + self.admin.session_collection + "/" + long_collection_name)
        full_logical_path = self.admin.session_collection + "/" + long_collection_name + "/" + filename
        # assertions
        self.admin.assert_icommand('ils -L ' + full_logical_path, 'STDERR_SINGLELINE', 'does not exist')  # should not be listed
        self.admin.assert_icommand('iput -K ' + filepath + ' ' + full_logical_path)  # iput
        self.admin.assert_icommand('ils -L ' + full_logical_path, 'STDOUT_SINGLELINE', filename)  # should be listed
        file_vault_full_path = os.path.join(self.admin.get_vault_session_path(), long_collection_name, filename)
        # method 1
        self.admin.assert_icommand('ichksum -K ' + full_logical_path, 'STDOUT_MULTILINE',
                                   ['Total checksum performed = 1, Failed checksum = 0',
                                    'sha2:' + orig_digest])  # ichksum
        # method 2
        self.admin.assert_icommand("iquest \"select DATA_CHECKSUM where DATA_NAME = '%s'\"" % filename,
                                   'STDOUT_SINGLELINE', ['DATA_CHECKSUM = sha2:' + orig_digest])  # iquest
        # method 3
        self.admin.assert_icommand('ils -L ' + long_collection_name, 'STDOUT_SINGLELINE', filename)  # ils
        self.admin.assert_icommand('ifsck -K ' + file_vault_full_path)  # ifsck
        # change content in vault
        with open(file_vault_full_path, 'r+t') as f:
            f.seek(0)
            print("x", file=f, end='')
        self.admin.assert_icommand('ifsck -K ' + file_vault_full_path, 'STDOUT_SINGLELINE', ['CORRUPTION', 'checksum not consistent with iRODS object'])  # ifsck
        # change size in vault
        lib.cat(file_vault_full_path, 'extra letters')
        new_digest = lib.file_digest(file_vault_full_path, 'sha256', encoding='base64')
        self.admin.assert_icommand('ifsck ' + file_vault_full_path, 'STDOUT_SINGLELINE', ['CORRUPTION', 'not consistent with iRODS object'])  # ifsck
        # unregister, reregister (to update filesize in iCAT), recalculate checksum, and confirm
        self.admin.assert_icommand('irm -U ' + full_logical_path)
        self.admin.assert_icommand('ireg ' + file_vault_full_path + ' ' + full_logical_path)
        self.admin.assert_icommand('ifsck -K ' + file_vault_full_path, 'STDOUT_SINGLELINE', ['WARNING: checksum not available'])  # ifsck
        self.admin.assert_icommand('ichksum -f ' + full_logical_path, 'STDOUT_MULTILINE',
                                   ['Total checksum performed = 1, Failed checksum = 0',
                                    'sha2:' + new_digest])
        self.admin.assert_icommand('ifsck -K ' + file_vault_full_path)  # ifsck
        # local cleanup
        os.remove(filepath)