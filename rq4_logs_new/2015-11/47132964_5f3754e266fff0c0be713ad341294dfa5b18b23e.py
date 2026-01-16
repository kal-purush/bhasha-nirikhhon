import cStringIO
import logging, json
import traceback
from logging import getLogger

try:
    import pycurl
except ImportError, e:
    print("Error: {}\n Please install pycurl.")


class Apiaxle():

    VERSION = 'v1'

    def __init__(self, host='localhost', port=3000):
        self.host = host
        self.port = port
        self.api_url = "http://{}:{}/{}".format(self.host, self.port, self.VERSION)
        self.curl_timeout = 5

    def __do_curl(self, path, method=None, payload=None):
        url = self.api_url + path

        header = "Content-Type: application/json"
        buf = cStringIO.StringIO()  # initialize buffer to write curl results to
        c = pycurl.Curl()
        c.setopt(pycurl.HTTPHEADER, [header])
        c.setopt(pycurl.WRITEFUNCTION, buf.write)
        c.setopt(pycurl.URL, str(url))
        c.setopt(pycurl.CONNECTTIMEOUT, self.curl_timeout)

        # pycurl auto swtiches to post when this is added
        if payload or method == "POST":
            if not payload:
                payload = {}
            c.setopt(pycurl.POSTFIELDS, json.dumps(payload))

        # forcing put even with post fields. This may not work
        if method == 'PUT':
            c.setopt(pycurl.CUSTOMREQUEST, "PUT")
        elif method == 'DELETE':
            c.setopt(pycurl.CUSTOMREQUEST, 'DELETE')

        if url.startswith('https'):
            c.setopt(pycurl.PORT, 443)
            c.setopt(pycurl.SSL_VERIFYPEER, 0)  # to make ssl work for the time being
            c.setopt(pycurl.SSL_VERIFYHOST, 0)
        try:
            c.perform()
        except Exception, e:
            return None, None, None

        statusCode = c.getinfo(pycurl.HTTP_CODE)
        error = c.errstr()
        message = buf.getvalue()  # get results do nothing yet

        return statusCode, message, error

    def get(self, path):
        status, body, error = self.__do_curl(path, 'GET')
        if status == 200:
            try:
                return json.loads(body)
            except Error, e:
                logging.error(e)
        else:
            logging.error(body)

        return None

    def post(self, path, payload):
        status, body, error = self.__do_curl(path, 'POST', payload)
        if status == 200:
            try:
                return json.loads(body)
            except Error, e:
                logging.error(e)
        else:
            logging.error(body)

        return None

    def put(self, path, payload={}):
        status, body, error = self.__do_curl(path, 'PUT', payload)
        if status == 200:
            try:
                return json.loads(body)
            except Error, e:
                logging.error(e)
        else:
            logging.error(body)

        return None

    def apis(self):
        return self.get('/apis').get('results')

    def keys(self):
        return self.get('/keys').get('results')

    def new_api(self, name, **kwargs):
        self.post('/api/{}'.format(name), kwargs)
        return self.api(name)

    def new_key(self, key):
        self.post('/key/{}'.format(key), {})
        return self.key(key)

    def link_key(self, api, key):
        return self.put('/api/{api}/linkkey/{key}'.format(api=api, key=key))

    def api(self, name):
        return Api(self.get('/api/{}'.format(name)).get('results'), name)

    def key(self, key):
        return Api(self.get('/key/{}'.format(key)).get('results'), key)


class Api():

    def __init__(self, api, api_name, **kwargs):
        self._api_name = api_name
        for key in api:
            setattr(self, key, api[key])

        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __str__(self):
        return self._api_name


class Key():

    def __init__(self, api_key, key_text, **kwargs):
        self._key_text = key_text
        for key in api_key:
            setattr(self, key, api_key[key])

        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __str__(self):
        return self._key_text