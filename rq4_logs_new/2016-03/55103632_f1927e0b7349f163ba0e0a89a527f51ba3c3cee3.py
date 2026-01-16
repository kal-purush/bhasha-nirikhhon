#!/usr/bin/python
#
# (c) Copyright 2016 Hewlett Packard Enterprise Development LP
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

from mininet.node import Host
from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo
from opsvsi.opsvsitest import OpsVsiTest, OpsVsiLink, VsiOpenSwitch


class ShowVersionDetailSysdCtTest(OpsVsiTest):
    def setupNet(self):
        # if you override this function, make sure to
        # either pass getNodeOpts() into hopts/sopts
        # of the topology that you build or into
        # addHost/addSwitch calls
        host_opts = self.getHostOpts()
        switch_opts = self.getSwitchOpts()
        system_topo = SingleSwitchTopo(k=0, hopts=host_opts,
                                       sopts=switch_opts)
        self.net = Mininet(system_topo, switch=VsiOpenSwitch,
                           host=Host, link=OpsVsiLink,
                           controller=None, build=True)
        self.s1 = self.net.switches[0]

    def check_show_version_detail_sysd_ct(self):
        output = self.s1.ovscmd('ovsdb-client dump Package_Info')
        assert "ops-sysd" in output, "ops-sysd was not found in \
        Package_Info table"


class TestRunner:
    @classmethod
    def setup_class(cls):
        cls.test = ShowVersionDetailSysdCtTest()

    @classmethod
    def teardown_class(cls):
        cls.test.stopNet()
        cls.test = None

    def test_show_version_detail_sysd_ct(self):
        return self.test.check_show_version_detail_sysd_ct()