import logging
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


class Ml2Cleaner():
    def __init__(self, controller_ip="localhost", username="admin",
                 password=None, tenant="admin", port="5000"):
        self.controller_ip = controller_ip
        self.userName = username
        self.password = password
        self.tenantName = tenant
        self.authPort = port
        self.protocol = 'http'
        self.session = requests.Session()
        self.token = self._get_token()
        self.token_header = {'Content-Type': 'application/json',
                             'Accept': 'application/json',
                             'User-Agent': 'python-neutronclient',
                             'X-Auth-Token': '%s' % self.token}
        self.tenantId = self._get_tenant_id()

    def _get_token(self):
        try:
            url = "%s://%s:%s/v2.0/tokens" % (self.protocol,
                                              self.controller_ip,
                                              self.authPort)
            headers = {'Content-Type': 'application/json',
                       'Accept': 'application/json',
                       'User-Agent': 'python-keystoneclient'}
            data = '{"auth": {"tenantName": "%s", '\
                   '"passwordCredentials":{"username": "%s", "password":'\
                   '"%s"}}}' % (self.tenantName, self.userName, self.password)
            logging.debug('Getting token')
            resp = self.session.post(url,
                                     data=data,
                                     headers=headers, timeout=TIMEOUT)
            if resp.status_code == requests.codes.ok:
                token = json.loads(resp.text)["access"]["token"]["id"]
                logging.debug('Token fetched')
                return token
            if resp.status_code == UN_AUTH:
                logging.warning('Token not fetched')
                exit()
        except exceptions.Exception as e:
            logging.warning(e)
            exit()

    def _get_tenant_id(self):
        try:
            url = self.get_url('tenants')
            logging.debug('Getting tenant id')
            resp = self.session.get(url,
                                    headers=self.token_header,
                                    timeout=TIMEOUT)
            resp_body = json.loads(resp.text)
            tenants = resp_body['tenants']
            for tenant in tenants:
                if tenant['name'] == self.tenantName:
                    tenant_id = tenant['id']
            logging.debug('Tenant id is %s' % tenant_id)
            return tenant_id
        except exceptions.Exception as e:
            logging.warning(e)
            exit()

    def get_url(self, resource):
        if resource in ['ports', 'routers', 'networks']:
            auth_port = '9696'
            url_str = "%s://%s:%s/v2.0/%s.json"
        if resource in ['tenants']:
            auth_port = '35357'
            url_str = "%s://%s:%s/v2.0/%s"
        if resource in ['servers']:
            auth_port = '8774'
            return "%s://%s:%s"\
                   "/v2/%s/%s/"\
                   "detail?all_tenants=1" % (self.protocol,
                                             self.controller_ip,
                                             auth_port,
                                             self.tenantId,
                                             resource)
        return url_str % (self.protocol,
                          self.controller_ip,
                          auth_port, resource)

    def get_resource_url(self, url, resource_type, resource):
        return url.split('.json')[0]+'/'+resource+'.json'

    def get_port_url(self, device_id):
        return self.get_url('ports') + '?' + 'device_id=%s' % device_id

    def remove_interface_and_gateway(self, url, resource_type, router_id):
        resource_url = self.get_port_url(router_id)
        update_url = None
        try:
            resp = self.session.get(resource_url,
                                    headers=self.token_header,
                                    timeout=TIMEOUT)
            if resp.status_code == requests.codes.ok:
                ports_data = json.loads(resp.text)
                ports = ports_data['ports']
                for port in ports:
                    if port['device_owner'] in 'network:router_gateway':
                        update_url = self.get_resource_url(url,
                                                           resource_type,
                                                           router_id)
                        data = '{"router": {"external_gateway_info": {}}}'
                    if port['device_owner'] in 'network:router_interface':
                        subnet_id = port['fixed_ips'][0]['subnet_id']
                        update_url = self.get_resource_url(url,
                                                           resource_type,
                                                           router_id).\
                            split('.jso')[0] + '/remove_router_interface.json'
                        data = '{"subnet_id":"%s"}' % subnet_id
                    if update_url:
                        resp = self.session.put(update_url,
                                                data=data,
                                                headers=self.token_header,
                                                timeout=TIMEOUT)
            else:
                logging.warning(json.loads(resp.text))
        except exceptions.Exception as e:
            logging.warning(e)
            exit()

    def cleanup_resources(self, *args):
        for arg in args:
            print 'Cleaning %s...' % arg
            url = self.get_url(arg)
            try:
                resp = self.session.get(url,
                                        headers=self.token_header,
                                        timeout=TIMEOUT)
                if resp.status_code == requests.codes.ok:
                    resources = json.loads(resp.text)
                    i = 0
                    resource_list = []
                    while i != len(resources[arg]):
                        resource_list.append(str(resources[arg][i]["id"]))
                        i += 1
                elif resp.status_code == UN_AUTH:
                    logging.warning("Unauthorized Access")
                    exit()
                for resource in resource_list:
                    if arg not in ['servers']:
                        durl = url.split('.json')[0]+'/'+resource+".json"
                    else:
                        durl = url.split('detail')[0]+'/'+resource
                    if arg in ['routers']:
                        self.remove_interface_and_gateway(url, arg, resource)
                    resp = self.session.delete(durl,
                                               headers=self.token_header,
                                               timeout=TIMEOUT)
                    if resp.status_code == requests.codes.ok:
                        pass
                    if resp.status_code == UN_AUTH:
                        logging.warning("Unauthorized Access")
                        exit()
            except exceptions.Exception as e:
                    logging.warning(e)
                    exit()


def main():
    obj = Ml2Cleaner('10.101.1.40', 'admin', 'noir0123', 'admin')
    obj.cleanup_resources('servers', 'routers', 'ports', 'networks')


if __name__ == '__main__':
    main()