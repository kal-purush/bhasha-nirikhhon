# gemapi utils
from threading import Thread
import websocket
import signal
import sys
import json

class GemKeyReader():

	def getKey(self):
		file = open('gemapikey')
		return file.read()

	def getSecret(self):
		file = open('gemapisecret')
		return file.read()

class GemSocket(Thread):
	def __init__(self, onMessage, onError = None):
		Thread.__init__(self)
		self.daemon = True
		self.onMessage = onMessage
		self.onError = onError
		signal.signal(signal.SIGINT, self.stop)

	def run(self):
		self._ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/BTCUSD", on_message=self.parseMessage, on_error=self.parseError)
		self._ws.run_forever(ping_interval=5)

	def parseMessage(self, ws, message):
		data = json.loads(message)
		event = data['events'][0]
		if event['type'] == 'trade':
			ms = data['timestampms']
			price = event['price']

			result = {
				'timestampms': ms,
				'price': price
			}
			self.onMessage(result)

	def parseError(self, ws, error):
		data = json.loads(error)
		self.onError(data)

	def stop(self, signal = None, frame = None):
		self._ws.keep_running = False
