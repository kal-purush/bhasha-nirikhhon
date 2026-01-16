from datetime import datetime
import time
import json
import requests
import itertools
import apikeys
from terminalcolors import *

_current_milli_time = lambda: int(round(time.time() * 1000))

class Api42:

	def __init__(self, uid, secret):
		self.token = None
		self.expires = 0
		self.apiLimit = int(0.5 * 1000)								# API limit is in milliseconds (0.5 seconds times 1000)
		self.lastCall = 0
		self.endpoint = 'https://api.intra.42.fr'
		self.headers = {
			'Authorization': 'Bearer',
			'content-type': 'application/x-www-form-urlencoded'
		}
		self.authData = {
			'client_id': uid,
			'client_secret': secret,
			'grant_type': 'client_credentials'
		}
		self.methodDict = {
			'GET': requests.get,
			'POST': requests.post
		}


	def makeRequest(self, endpoint):
		#	If a new token is needed, update it
		self.updateToken()

		#	Make the actual request
		return self.get(endpoint, None, self.headers)


	def updateToken(self):
		#	Check whether token needs updating
		if _current_milli_time() >= self.expires:
			r = self.post('/oauth/token', self.authData, None)
			if r is None:
				return

			#	Update token, expiry time, and authorization header
			self.token = r[0]['access_token']
			self.expires = (r[0]['expires_in'] * 1000) + _current_milli_time()
			self.headers['Authorization'] = 'Bearer ' + self.token
			print(IGREEN + "Token Updated!" + ENDCOLOR)


	def post(self, uri, data, headers):
		return self.send(uri, 'POST', data, headers);


	def get(self, uri, data, headers):
		return self.send(uri, 'GET', data, headers);


	def send(self, uri, method, data, headers):
		#	initializing send request
		url 		= self.endpoint + uri
		requestFunc	= self.methodDict.get(method, None)

		#	Making the request - only handles get and post requests for now
		rsp, returnData = self._request(url, data, headers, requestFunc)
		if rsp is None:
			return None

		#	Loop to get all data
		while 'next' in rsp.links:
			url = rsp.links['next']['url']
			rsp, tmpData = self._request(url, data, headers, requestFunc)
			if rsp is None:
				return None
			returnData += tmpData
		return returnData


	#	Waits, makes actual request, then returns the response and returnData list
	#	Should be a private function
	def _request(self, url, data, headers, requestFunc):
		#	Pausing for the api limiter
		self._waitForRequest();

		#	Making the request - only handles get and post requests for now
		print(IYELLOW + "Requesting data from... " + ICYAN + url + ENDCOLOR, end='')
		rsp = requestFunc(url, data=data, headers=headers)

		#	Error handling
		if (rsp is None) or rsp.status_code != 200:
			print(BRED + " ...Failed!" + ENDCOLOR)
			return None, None

		#	Returning the response object AND the list of returnData
		print(BGREEN + " ...Success!" + ENDCOLOR)
		returnData = rsp.json()
		returnData = [returnData] if type(returnData) is dict else returnData
		return rsp, returnData

	#	A helper function that waits until the next available time to make the api call
	def _waitForRequest(self):
		while (_current_milli_time() - self.apiLimit) < self.lastCall:
			pass
		self.lastCall = _current_milli_time()

import pprint
myapi = Api42(apikeys.uid, apikeys.secret)
p = myapi.makeRequest('/v2/users/24794/projects_users?range[final_mark]=50,125')
print(p)