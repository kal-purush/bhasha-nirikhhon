import requests
import json
import exceptions

SUCCESS = 1
FAILURE = 0
CREATED = 201
ACCEPTED = 202
UN_AUTH = 401
DELETED = 204
CONFLICT = 409
TIMEOUT = 30


class GbpCleanUp(object):
    controller_ip = None
    username = None
    password = None
    tenant_name = None
    auth_port = None
    resource_port = None
    protocol = None
    session = None
    token = None
    token_header = None
    tenant_id = None

    def __init__(self, controller_ip="localhost", username="admin",
                 password=None, tenant="admin", auth_port="5000",
                 resource_port="9696"):
        GbpCleanUp.controller_ip = controller_ip
        GbpCleanUp.username = username
        GbpCleanUp.password = password
        GbpCleanUp.tenant_name = tenant
        GbpCleanUp.auth_port = auth_port
        GbpCleanUp.resource_port = resource_port
        GbpCleanUp.protocol = 'http'
        GbpCleanUp.session = requests.Session()
        GbpCleanUp.token = self._get_token()
        GbpCleanUp.token_header = {'Content-Type': 'application/json',
                                   'Accept': 'application/json',
                                   'User-Agent': 'python-neutronclient',
                                   'X-Auth-Token': '%s' % self.token}
        GbpCleanUp.tenant_id = self._get_tenant_id()

    def _get_token(self):
        try:
            url = Url('tokens')
            headers = {'Content-Type': 'application/json',
                       'Accept': 'application/json',
                       'User-Agent': 'python-keystoneclient'}
            data = '{"auth": {"tenantName": "%s", '\
                   '"passwordCredentials":{"username": "%s", "password":'\
                   '"%s"}}}' % (self.tenant_name, self.username, self.password)

            resp = self.session.post(url,
                                     data=data,
                                     headers=headers, timeout=TIMEOUT)
            if resp.status_code == requests.codes.ok:
                token = json.loads(resp.text)["access"]["token"]["id"]
                return token
            if resp.status_code == UN_AUTH:
                exit()
        except exceptions.Exception as e:
            print e
            exit()

    def _get_tenant_id(self):
        try:
            url = Url('tenants')
            resp = self.session.get(url,
                                    headers=self.token_header,
                                    timeout=TIMEOUT)
            resp_body = json.loads(resp.text)
            tenants = resp_body['tenants']
            for tenant in tenants:
                if tenant['name'] == self.tenant_name:
                    tenant_id = tenant['id']
            return tenant_id
        except exceptions.Exception as e:
            print e

    def clean(self, *args):
        component_types = args
        for component_type in component_types:
            print "Cleaning %s ..." % component_type
            components = ListComponents(component_type).list_components()
            for component in components:
                component.delete()


class GbpComponent():
    def __init__(self, component_type=None, component_id=None):
        self.component_type = component_type
        self.component_id = component_id

    def delete(self):
        delete_url = Url(self.component_type, self.component_id)
        Client(delete_url, 'DELETE').get_response()

    def show(self):
        pass

    def update(self):
        pass


class Client(GbpCleanUp):
    def __init__(self, url=None, http_method=None, data=None):
        self.url = url
        self.data = data
        self.http_method = http_method

    def get_response(self):
        try:
            if self.http_method == 'GET':
                resp = self.session.get(self.url,
                                        headers=self.token_header,
                                        timeout=TIMEOUT)
                return json.loads(resp.text)
            if self.http_method == 'DELETE':
                resp = self.session.delete(self.url,
                                           headers=self.token_header,
                                           timeout=TIMEOUT)
            if resp.status_code == UN_AUTH:
                exit()

        except exceptions.Exception as e:
            print e
            exit()


class ListComponents():
    def __init__(self, component_type=None):
        self.component_type = component_type

    def list_components(self):
        url = Url(self.component_type)
        response_body = Client(url, 'GET').get_response()

        components_list = []
        for component in response_body[self.component_type]:
            components_list.append(GbpComponent(self.component_type,
                                                component['id']))
        return components_list


class Url(GbpCleanUp):
    def __init__(self, url_type=None, *args):
        self.url_type = url_type
        self.args = args

    def __repr__(self):
        return self._get_url()

    def _get_url(self):
        url_str = "%s://%s:%s/v2.0/" % (self.protocol,
                                        self.controller_ip,
                                        "%s")

        if self.url_type in ['tokens', 'tenants']:
            return url_str % self.auth_port + self.url_type

        else:
            a = url_str % self.resource_port
            b = 'grouppolicy/%s' % self.url_type
            url_str = a + b
            for arg in self.args:
                url_str += '/%s' % arg
            return url_str + ".json"


GbpCleanUp('10.30.120.52',
           'admin',
           'noir0123',
           'admin').clean('policy_rule_sets',
                          'policy_rules',
                          'policy_classifiers',
                          'policy_actions')