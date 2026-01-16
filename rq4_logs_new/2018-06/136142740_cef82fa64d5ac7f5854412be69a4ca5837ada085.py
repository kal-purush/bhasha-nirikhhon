# -*- encoding: utf-8 -*-
import sys
import json
import argparse

from common import utils
from message_handle import Messager
from hwctl.executor.openbmc_inventory import OpenBMCInventoryTask
from hwctl.executor.openbmc_power import OpenBMCPowerTask
from hwctl.inventory import DefaultInventoryManager
from hwctl.power import DefaultPowerManager

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import logging
logger = logging.getLogger('xcathwctl')
handler = logging.FileHandler("/var/log/xcathwctl.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class OpenBMCManager():
    """OpenBMCManager

    :param nodesinfo: A dict contains nodes' name, bmc, bmcip, username, password
    :param verbose: A boolean indicating show verbose information
    """

    def __init__(self, nodesinfo, verbose=False):
        """Inits OpenBMCManager with nodesinfo"""
        self.nodesinfo = nodesinfo
        self.messager = Messager(verbose)
        self.debugmode = verbose
        self.verbose = False

        if self.debugmode:
            logger.setLevel(logging.DEBUG)

    def _get_runner(self, command):

        return {
            'rinv': OpenBMCInventoryTask(self.nodesinfo, callback=self.messager, debugmode=self.debugmode, verbose=self.verbose),
            'power': OpenBMCPowerTask(self.nodesinfo, callback=self.messager, debugmode=self.debugmode, verbose=self.verbose),
        }.get(command, None)

    def _get_result(self):

        return json.dumps(self.result_dict) 

    def inventory(self):

        runner = self._get_runner('rinv') 
        self.result_dict = DefaultInventoryManager().get_inventory_info(runner, ['all'])
        return self._get_result()

    def inv_cpu(self):

        runner = self._get_runner('rinv')
        self.result_dict = DefaultInventoryManager().get_inventory_info(runner, ['cpu'])
        return self._get_result()

    def inv_model(self):

        runner = self._get_runner('rinv')
        self.result_dict = DefaultInventoryManager().get_inventory_info(runner, ['model'])
        return self._get_result()

    def inv_serial(self):

        runner = self._get_runner('rinv')
        self.result_dict = DefaultInventoryManager().get_inventory_info(runner, ['serial'])
        return self._get_result()

    def power_state(self):
        """Get host state.

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().get_power_state(runner) 
        return self._get_result()

    def power_on(self):
        """Set host power status on.

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().set_power_state(runner, power_state='on')
        return self._get_result()

    def power_off(self):
        """Set host power status off.

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().set_power_state(runner, power_state='off') 
        return self._get_result()

    def power_softoff(self):
        """Set host power status off(soft).

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().set_power_state(runner, power_state='softoff')
        return self._get_result()

    def power_reset(self):
        """Reset host.

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().reboot(runner, optype='reset')
        return self._get_result()

    def power_boot(self):
        """Boot host..

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().reboot(runner, optype='boot')
        return self._get_result()

    def power_bmc_state(self):
        """Get BMC status.

        :rtype: json
        :return: all nodes' data and retrun code
        """ 
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().get_bmc_state(runner)
        return self._get_result()

    def power_bmc_reboot(self):
        """Reboot BMC.

        :rtype: json
        :return: all nodes' data and retrun code
        """
        runner = self._get_runner('power')
        self.result_dict = DefaultPowerManager().reboot_bmc(runner)
        return self._get_result()