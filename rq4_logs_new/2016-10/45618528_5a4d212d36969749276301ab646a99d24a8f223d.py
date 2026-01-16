#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright 2016 Telefónica Investigación y Desarrollo, S.A.U
#
# This file is part of FI-WARE project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at:
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.
#
# For those usages not covered by the Apache version 2.0 License please
# contact with opensource@tid.es
#

from util.configuration import Config
from model.tier import Tier


class Template():

    """This class represents the template."""
    def __init__(self, template_name, template_description, tiers=None):
        """
        :param template_name: the template name
        :param template_description: the template description
        :param tiers: a set of tiers
        :return: nothing
        """
        self.template_name = template_name
        self.template_description = template_description
        self.tiers = tiers

    def add_tiers(self, tierDtos):
        """
        It adds tiers to the template from a XML
        :param tierDtos: XML with the tier information
        :return: nothing
        """
        if isinstance(tierDtos, list):
            for tierXML in tierDtos:
                self._add_tier(tierXML)
        else:
            self._add_tier(tierDtos)

    def _add_tier(self, tierXML):
        if not self.tiers:
            self.tiers = []
        tier = Tier(tierXML["name"], tierXML["flavour"], tierXML["image"])
        tier.add_products_from_xml(tierXML["productReleaseDtos"], self.template_id)
        self.tiers.append(tier)

    def create_env_template(self):
        """
        It creates a template into murano
        :return:
        """
        murano_template = Config.Clients.create_env_template(self.template_name,
                                                             self.template_description)
        self.template_id = murano_template.id