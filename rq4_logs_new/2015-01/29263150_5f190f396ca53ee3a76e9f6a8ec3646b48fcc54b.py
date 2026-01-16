import requests
from urlparse import urljoin
import purl
from functools import partial

PIVOTAL_TRACKER_TOKEN = '2a2c2003c8e45a643ad8af2b066b3e71'

class NullReturnedError(Exception):
    pass

class ResourceNotFound(Exception):
    pass

HTTP_VERBS = ('head', 'options', 'get', 'post', 'put', 'patch', 'delete')

class Request(object):
    """docstring for Request"""
    def __init__(self, method=None, session=None,
            auth_callable=(lambda x: x), base_url=None,
            uriparts=None, sent=False):
        self.method = method
        self.session = session
        self.auth_callable = auth_callable
        self.base_url = base_url
        self.uriparts = uriparts or []
        self.baseparts = self.uriparts
        self.sent = sent
        self.called = False

    def augment_request(self, arg, reset=False):
        print "augment_request", reset, arg, self.uriparts, self.baseparts
        if reset:
            self.uriparts = self.baseparts
            return Request(
                method=self.method,
                session=self.session,
                auth_callable=self.auth_callable,
                base_url=self.base_url,
                uriparts=self.uriparts,
                sent=False
            )
        else:
            self.uriparts.append(arg)
            return self

    @property
    def resource_key(self):
        def filter_(arg):
            try:
                _ = int(arg)
            except ValueError:
                return arg not in HTTP_VERBS
            else:
                return False

        return [p for p in self.uriparts if filter_(p)][-1]

    def _send(self, method, **kwargs):
        return_json = kwargs.pop('return_json', True)

        url = purl.URL(self.base_url)
        # print resource_ids
        print "entered send", self.uriparts
        for p in self.uriparts:
            url = url.add_path_segment(p)

        def hook(r, *args, **kwargs):
            r.resource_key = self.resource_key

        hooks = {"response": hook}

        request = requests.Request(method, url, hooks=hooks, **kwargs)

        if self.session is None:
            self.session = requests.Session()

        prepped = self.auth_callable(self.session.prepare_request(request))

        resp = self.session.send(prepped)

        print prepped, prepped.url
        self.sent = True


        if return_json:
            return resp.json()
        else:
            return resp

    def __getattr__(self, k):
        try:
            return object.__getattr__(self, k)
        except AttributeError:
            return self.augment_request(k, reset=self.sent)

    def __call__(self, id=None, **kwargs):
        self.called = True
        if self.uriparts[-1] in HTTP_VERBS:
            method = self.uriparts.pop()
            if id is not None:
                self.augment_request(id)
            return self._send(method, **kwargs)
        else:
            if id is not None:
                return self.augment_request(id, reset=self.sent)
            else:
                return self

    def reset(self):
        self.uriparts = self.baseparts
        print self.uriparts


class Client(object):
    def __init__(self, token, base_url):
        self.token = token
        self.base_url = base_url
        self.session = requests.Session()

    def token_auth(self, request):
        request.headers.update({
            'x-trackertoken': self.token
        })
        return request

    def request(self, **kwargs):
        return Request(session=self.session, auth_callable=self.token_auth,
            base_url=self.base_url, **kwargs)

    def __getattr__(self, k):
        try:
            return object.__getattr__(self, k)
        except AttributeError:
            request = self.request(uriparts=[k])
            return request


# class PivotalRequest(object):
#     def __init__(self, token, uriparts=None):
#         self.token = token
#         self.base_url = 'https://www.pivotaltracker.com/services/v5'
#         if uriparts is None:
#             self.uriparts = []
#         else:
#             self.uriparts = uriparts

#     def __getattr__(self, k):
#         try:
#             return object.__getattr__(self, k)
#         except AttributeError:
#             self.uriparts.append(k)
#             return PivotalRequest(token=self.token,
#                 uriparts=self.uriparts)

#     def __call__(self, **kwargs):
#         method = kwargs.pop('method', 'GET')
#         resource_type = kwargs.pop('resource_type', None)

#         resource_ids = {k[:-3]: v for k, v in kwargs.iteritems()
#             if k.endswith("_id")}

#         for resource in self.uriparts:
#             resource_key = resource[:-1] # e.g. projects => project
#             resource_id = resource_ids.get(resource)
#             if resource_id:
#                 pos = self.uriparts.index(resource)
#                 self.uriparts.insert(pos + 1, resource_id)
#         # project_id = kwargs.pop('project_id', None)
#         # if project_id is not None:
#         #     self.uriparts = ('projects', project_id) + self.uriparts

#         if resource_type is not None:
#             self.uriparts.append(resource_type)

#         id = kwargs.pop('id', None)
#         if id is not None:
#             self.uriparts.append(id)

#         headers = kwargs.get('headers', {})
#         headers.update({
#             'x-trackertoken': self.token,
#             'Content-Type': 'application/json'
#         })

#         url = purl.URL(self.base_url)

#         print resource_ids
#         print self.uriparts
#         for p in self.uriparts:
#             url = url.add_path_segment(p)

#         return_json = kwargs.pop('return_json', True)

#         try:
#             resp = requests.request(method, url.as_string(),
#                 headers=headers, **kwargs)
#         except Exception, e:
#             print "{0} got error {1}".format(url, e)
#             raise
#         else:
#             if resp.status_code == 404:
#                 raise ResourceNotFound("{0}".format(resp.request.url))

#         if return_json:
#             return resp.json()
#         else:
#             return resp


# class Client(PivotalRequest):
#     def __init__(self, token, *args, **kwargs):
#         PivotalRequest.__init__(self, token,
#             *args, **kwargs)

#     def search(self, project_id=None, query_string=None, **kwargs):
#         return super(Client, self).__call__(project_id=project_id,
#             resource_type='search', params={'query': query_string}, **kwargs)


# class Client(object):
#     """Simple Pivotal Tracker client"""

#     def __init__(self):
#         super(Client, self).__init__()
#         self.base_url = 'https://www.pivotaltracker.com/services/v5/'
#         self.url_pattern = urljoin(self.base_url, 'projects/{0}/{1}')

#     def _request(self, method, project_id, resource_type,
#             headers={}, params={}, data={}, return_json=True, **kwargs):

#         headers.update({
#             'x-trackertoken': PIVOTAL_TRACKER_TOKEN,
#             'Content-Type': 'application/json'
#         })

#         if resource_type == 'me':
#             url = urljoin(self.base_url, 'me')
#         else:
#             url = self.url_pattern.format(project_id, resource_type)

#         try:
#             resp = requests.request(method, url, headers=headers,
#                 params=params, data=data, **kwargs)
#         except Exception, e:
#             print "{0} got error {1}".format(url, e)
#             raise

#         if resp.status_code == 404:
#             raise ResourceNotFound("No resource found at %s" % resp.request.url)

#         rv = resp.json()
#         if rv == "null" or rv is None:
#             raise NullReturnedError("Response for %s was null" % url)
#         return rv if return_json else resp


#     def get(self, project_id, resource_type, **kwargs):
#         return self._request('get', project_id, resource_type, **kwargs)

#     def search(self, project_id, query_string):
#         return self.get(project_id, 'search', params={'query': query_string})

#     def post(self, project_id, data, resource_type='stories',
#             return_json=True, **kwargs):
#         return self._request('post', project_id, resource_type,
#             return_json=return_json, **kwargs)

#     def me(self, **kwargs):
#         return self._request('get', None, 'me', **kwargs)


if __name__ == '__main__':
    # client = Client()
    # # resp = client.get('1138154', 'memberships', return_json=False)
    # resp = client.zod(return_json=False)
    # # resp = client.search('1138154', 'type:release', return_json=False)
    # print resp.status_code, resp.request.url, resp.content
    # print list((p['username'], p['id']) for p in (m['person'] for m in resp))
    client = Client(base_url='https://www.pivotaltracker.com/services/v5/',
        token=PIVOTAL_TRACKER_TOKEN)
    req = client.request()
    # resp = req.projects('1138154').labels(9308508).get()
    # resp = req.projects('1138154').labels.get()
    # resp = client.projects('1138154').get()
    project = client.projects('1138154')
    r1 = project.labels.get()
    r2 = project.labels('9308508').get()
    r3 = project.members.get()
    # resp = req.projects.get()
    print r1.content, r1.resource_key
    print r2.content, r2.resource_key
    print r3.content, r3.resource_key