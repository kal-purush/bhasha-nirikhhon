# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016: Alignak contrib team, see AUTHORS.txt file for contributors
#
# This file is part of Alignak contrib projet.
#
# Alignak is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Alignak is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Alignak.  If not, see <http://www.gnu.org/licenses/>.

"""
This file is used to test the arbiter configuration change checking
"""
import os
import shlex
import time
import subprocess
import json
import unittest2
from calendar import timegm
from datetime import datetime

from alignak_module_backend.arbiter.module import AlignakBackendArbiter
from alignak.objects.config import Config
from alignak.objects.module import Module
from alignak_backend_client.client import Backend


class TestArbiterConfigCheck(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set test mode for alignak backend
        # Uncomment to log the bakend REST API
        # os.environ['TEST_ALIGNAK_BACKEND'] = '1'
        os.environ['ALIGNAK_BACKEND_MONGO_DBNAME'] = 'alignak-module-backend-test'

        # Delete used mongo DBs
        print ("Deleting Alignak backend DB...")
        exit_code = subprocess.call(
            shlex.split(
                'mongo %s --eval "db.dropDatabase()"' % os.environ[
                    'ALIGNAK_BACKEND_MONGO_DBNAME'])
        )
        assert exit_code == 0

        cls.p = subprocess.Popen(['uwsgi', '--plugin', 'python', '-w', 'alignakbackend:app',
                                  '--socket', '0.0.0.0:5000',
                                  '--protocol=http', '--enable-threads', '--pidfile',
                                  '/tmp/uwsgi.pid'])
        time.sleep(3)

        cls.backend = Backend('http://127.0.0.1:5000')
        cls.backend.login("admin", "admin", "force")
        realms = cls.backend.get_all('realm')
        for cont in realms['_items']:
            cls.realm_all = cont['_id']

        # Get admin user
        users = cls.backend.get_all('user')
        for user in users['_items']:
            if user['name'] == 'admin':
                cls.user_admin = user

        # add commands
        data = json.loads(open('cfg/command_ping.json').read())
        data['_realm'] = cls.realm_all
        data_cmd_ping = cls.backend.post("command", data)
        data = json.loads(open('cfg/command_http.json').read())
        data['_realm'] = cls.realm_all
        data_cmd_http = cls.backend.post("command", data)

        # add 1 host
        data = json.loads(open('cfg/host_srv001.json').read())
        data['check_command'] = data_cmd_ping['_id']
        del data['realm']
        data['_realm'] = cls.realm_all
        cls.data_host = cls.backend.post("host", data)

        # add 2 services
        data = json.loads(open('cfg/service_srv001_ping.json').read())
        data['host'] = cls.data_host['_id']
        data['check_command'] = data_cmd_ping['_id']
        data['_realm'] = cls.realm_all
        cls.data_srv_ping = cls.backend.post("service", data)

        data = json.loads(open('cfg/service_srv001_http.json').read())
        data['host'] = cls.data_host['_id']
        data['check_command'] = data_cmd_http['_id']
        data['_realm'] = cls.realm_all
        cls.data_srv_http = cls.backend.post("service", data)

        # Start arbiter module
        modconf = Module()
        modconf.module_alias = "backend_arbiter"
        modconf.username = "admin"
        modconf.password = "admin"
        modconf.api_url = 'http://127.0.0.1:5000'

        # Update default check timers
        # check every x min if config in backend changed, if yes it will reload it
        # Default, every 5 minutes
        modconf.verify_modification = 1
        # verify_modification     5

        cls.arbmodule = AlignakBackendArbiter(modconf)
        cls.objects = cls.arbmodule.get_objects()

    @classmethod
    def tearDownClass(cls):
        cls.p.kill()

    def test_check(self):
        """Test configuration check - no changes"""

        # ---
        class Arbiter(object):
            conf = Config()
            pidfile = '/tmp/arbiter.pid'

        arb = Arbiter()

        if os.path.exists('/tmp/arbiter.pid'):
            os.remove('/tmp/arbiter.pid')

        now = timegm(datetime.utcnow().utctimetuple())

        # Get our existing host
        hosts = self.backend.get_all('host')
        self.assertEqual(len(hosts['_items']), 2)
        self.assertEqual(hosts['_items'][1]['name'], 'srv001')
        print("Host updated date: %s" % hosts['_items'][1]['_updated'])
        now_backend_fmt = datetime.utcfromtimestamp(now).strftime(self.arbmodule.backend_date_format)
        print("Now date: %s" % now_backend_fmt)

        # A bit later ...
        time.sleep(1)

        # No configuration check is done ... because it is not yet the moment to check.
        print("No configuration check")
        next_check = self.arbmodule.next_check
        print("Next check: %s" % (next_check))
        assert self.arbmodule.configuration_reload_required is False
        assert self.arbmodule.configuration_reload_changelog == []
        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check == next_check
        assert self.arbmodule.configuration_reload_required is False
        assert self.arbmodule.configuration_reload_changelog == []

        # Set next check in the past to force a configuration check
        # Set check date in the future to have no results
        # And configuration has not changed...
        print("Configuration check - nothing changed")
        self.arbmodule.next_check = now - 1
        self.arbmodule.time_loaded_conf = datetime.utcfromtimestamp(now + 1).strftime(self.arbmodule.backend_date_format)
        print("Next check: %s" % (next_check))
        assert self.arbmodule.configuration_reload_required is False
        assert self.arbmodule.configuration_reload_changelog == []
        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check > now
        assert self.arbmodule.configuration_reload_required is False
        assert len(self.arbmodule.configuration_reload_changelog) == 0

        # Set next check in the past to force a configuration check
        # Set check date in the past also to have some results
        print("Configuration check - get test setup created objects")
        self.arbmodule.next_check = now - 1
        self.arbmodule.time_loaded_conf = datetime.utcfromtimestamp(now - 5).strftime(self.arbmodule.backend_date_format)
        print("Next check: %s" % (next_check))
        assert self.arbmodule.configuration_reload_required is False
        assert self.arbmodule.configuration_reload_changelog == []
        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check > now
        assert self.arbmodule.configuration_reload_required is True
        assert len(self.arbmodule.configuration_reload_changelog) == 16
        # Nothing changed becaused only the host livestate got updated

        time.sleep(1.1)

        # Update an item in the backend
        # update the host with ls_acknowledged = True
        headers = {
            'Content-Type': 'application/json',
            'If-Match': self.data_host['_etag']
        }
        self.backend.patch('host/%s' % self.data_host['_id'],
                           {"ls_acknowledged": True},
                           headers, True)
        hosts = self.backend.get_all('host')
        self.assertEqual(len(hosts['_items']), 2)
        self.assertEqual(hosts['_items'][1]['name'], 'srv001')
        self.assertEqual(hosts['_items'][1]['ls_acknowledged'], True)
        print("Host updated date: %s" % hosts['_items'][1]['_updated'])
        now_backend_fmt = datetime.utcfromtimestamp(now).strftime(self.arbmodule.backend_date_format)
        print("Now date: %s" % now_backend_fmt)

        # Set next check in the past to force a configuration check
        # Set check date in the past also to have some results
        print("Configuration check - an host updated")
        now=time.time()
        self.arbmodule.next_check = now - 1
        self.arbmodule.time_loaded_conf = datetime.utcfromtimestamp(now - 1).strftime(self.arbmodule.backend_date_format)
        print("Next check: %s" % (next_check))
        # Reset change log
        self.arbmodule.configuration_reload_required = False
        self.arbmodule.configuration_reload_changelog = []
        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check > now
        assert self.arbmodule.configuration_reload_required is False
        assert len(self.arbmodule.configuration_reload_changelog) == 0
        # Nothing changed becaused only the host livestate got updated

        # Update an item in the backend
        # update the host alias
        headers = {
            'Content-Type': 'application/json',
            'If-Match': self.data_host['_etag']
        }
        self.backend.patch('host/%s' % self.data_host['_id'],
                           {"alias": "Changed host alias"},
                           headers, True)
        hosts = self.backend.get_all('host')
        self.assertEqual(len(hosts['_items']), 2)
        self.assertEqual(hosts['_items'][1]['name'], 'srv001')
        self.assertEqual(hosts['_items'][1]['alias'], "Changed host alias")
        print("Host updated date: %s" % hosts['_items'][1]['_updated'])
        now_backend_fmt = datetime.utcfromtimestamp(now).strftime(self.arbmodule.backend_date_format)
        print("Now date: %s" % now_backend_fmt)

        time.sleep(1)

        # Set next check in the past to force a configuration check
        # Set check date in the past also to have some results
        now = int(time.time())
        self.arbmodule.next_check = now - 1
        self.arbmodule.time_loaded_conf = datetime.utcfromtimestamp(now - 1).strftime(self.arbmodule.backend_date_format)
        assert self.arbmodule.configuration_reload_required is False
        assert self.arbmodule.configuration_reload_changelog == []
        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check > now
        assert self.arbmodule.configuration_reload_required is True
        assert len(self.arbmodule.configuration_reload_changelog) == 2
        print("Backend log report:")
        for log in self.arbmodule.configuration_reload_changelog:
            print(" - %s: %s" % (log['resource'], log['item']))
            if log['resource'] == 'backend-log':
                item = log['item']
                assert '_updated' in item
                assert item['level'] == 'ERROR'
                assert item['message'] == "The arbiter pid file (%s) is not available. " \
                                          "Configuration reload notification was not raised." \
                                          % arb.pidfile
        # No arbiter PID file exists!

        # Create a fake PID file
        f = open('/tmp/arbiter.pid', 'w')
        f.write(str(123456))
        f.close()

        # Set next check in the past to force a configuration check
        # And configuration has changed... why? Because ob the objects created in the setup
        # of this test. The tests is executing fast and then the test "Greated than or equal"
        # is matched!
        self.arbmodule.next_check = now - 1
        self.arbmodule.time_loaded_conf = datetime.utcfromtimestamp(now - 1).strftime(self.arbmodule.backend_date_format)
        # Now we are yet True and some log exist
        assert self.arbmodule.configuration_reload_required is True
        assert len(self.arbmodule.configuration_reload_changelog) == 2

        self.arbmodule.hook_tick(arb)
        print("Next check: %s / %s" % (next_check, self.arbmodule.next_check))
        assert self.arbmodule.next_check > next_check

        # Still true but only one more log
        assert self.arbmodule.configuration_reload_required is True
        assert len(self.arbmodule.configuration_reload_changelog) == 3
        print("Backend log report:")
        got_invalid_file = False
        for log in self.arbmodule.configuration_reload_changelog:
            print(" - %s: %s" % (log['resource'], log['item']))

            if log['resource'] == 'backend-log':
                item = log['item']
                assert '_updated' in item
                assert item['level'] == 'ERROR'
                if not got_invalid_file:
                    assert item['message'] == "The arbiter pid file (%s) is not available. " \
                                              "Configuration reload notification was not raised." \
                                              % arb.pidfile
                    got_invalid_file = True
                else:
                    assert item['message'] == "The arbiter pid (%s) stored in file (%s) is not " \
                                              "for an existing process. " \
                                              "Configuration reload notification was not raised." \
                                              % (123456, arb.pidfile)
        # No arbiter PID file exists remains in the log!
        # Invalid arbiter PID!