import http.client
import json


class TRAD:
    def __headers(self):
        return ({
             'Content-type' : 'application/json',
             'Hash' : self.__hash,
             'Auth' : self.__hash,
             'Client' : self.__client,
             'User' : self.__user,
             'Type' : self.__type,
             'Company' : self.__company
        })
    def __get(self, url: str)->dict:
        self.__conn.request('GET', url, '', self.__headers()) 
        response = self.__conn.getresponse()
        self.__last_response = response
        return response.read().decode()
    def __post(self, url: str, post:dict)->dict:
        self.__conn.request('POST', url, json.dumps(post), self.__headers()) 
        response = self.__conn.getresponse()
        self.__last_response = response
        return response.read().decode()
    def hello(self)->str:
        result = json.loads(self.__get('/hello'))
        try:
            self.__client = result['client']
            self.__last_failed = False
            return self.__client
        except (NameError, AttributeError):
            self.__last_failed = True
        return ''
    def login(self, login:str, password:str)->bool:
        result = json.loads(self.__post('/login', {'login': login,'password':password}))
        try:
            self.__auth = result['data']['auth']
            self.__user = result['data']['id']
            self.__last_failed = False
            return self.__user
        except (NameError, AttributeError):
            self.__last_failed = True
        return ''
    def standup(
      self,
      plan:str,
      blocking:str,
      yesterday:str,
      result:str,
      blocked:str,
      comment:str,
      feeling:str,
      timestamp:int,
      day:int
   )->bool:
        self.__last_failed = False
        result = json.loads(self.__post(
          '/hello',
          {
              'plan':plan,
              'blocking':blocking,
              'yesterday':yesterday,
              'result':result,
              'blocked':blocked,
              'comment':comment,
              'feeling':feeling,
              'timestamp':timestamp,
              'day':day
          }
        ))
        try:
            return result['client']
        except (NameError, AttributeError):
            self.__last_failed = True
        return ''
    def __init__(self):
        self.__last_failed = False
        self.__last_response = '';
        self.__hash = "null"
        self.__auth = "null"
        self.__client = "null"
        self.__user = "null"
        self.__company = "null"
        self.__type = "TPY0OP"
        self.__conn = http.client.HTTPSConnection('api.trad.gidigi.com', 443)