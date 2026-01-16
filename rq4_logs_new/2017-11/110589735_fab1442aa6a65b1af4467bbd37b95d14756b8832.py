# Copyright 2017 Rackspace, US Inc.
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

import functools

from oslo_config import cfg
from oslo_log import log as logging
import requests
from taskflow import task

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class BaseKeystoneTask(task.Task):
    """Base task to load code common to the tasks."""

    def __init__(self, **kwargs):
        super(BaseKeystoneTask, self).__init__(**kwargs)
        self.get = functools.partial(self._request, 'GET')
        self.post = functools.partial(self._request, 'POST')
        self.put = functools.partial(self._request, 'PUT')
        self.delete = functools.partial(self._request, 'DELETE')

    def _request(self, method, url, data=None, params=None):
        headers = {
            'Content-type': 'application/json',
            'User-Agent': 'Stress_Octavia_API'
        }

        url = '{}/{}'.format(CONF.keystone_authtoken.auth_url, str(url))
        LOG.debug("url = %s", url)
        LOG.debug("data = %s", data)
        LOG.debug("params = %s", str(params))
        r = requests.request(method, url, data=data,
                             params=params, headers=headers)
        LOG.debug("Keystone Response Code: {0}".format(r.status_code))
        LOG.debug("Keystone Response Body: {0}".format(r.content))
        LOG.debug("Keystone Response Headers: {0}".format(r.headers))

        return r


class GetToken(BaseKeystoneTask):
    """Task to get a keystone token."""

    def execute(self):

        data=('{{"auth":{{"identity":{{"methods":["password"],'
              '"password":{{"user":{{"name":"{name}",'
              '"domain":{{"name": "{domain}"}},'
              '"password":"{password}"}}}}}},'
              '"scope":{{"project":{{"name":"{proj}",'
              '"domain": {{"name": "{domain}"}}}}}}}}'
              '}}').format(name=CONF.keystone_authtoken.username,
                           password=CONF.keystone_authtoken.password,
                           proj=CONF.keystone_authtoken.project_name,
                           domain=CONF.keystone_authtoken.project_domain_name)


        r = self.post('/v3/auth/tokens', data)

        return r.headers['X-Subject-Token']