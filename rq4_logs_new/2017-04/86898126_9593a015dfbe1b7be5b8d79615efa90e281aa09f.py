'''
A minimal http server for basic audio feedback
'''

import atexit
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from SocketServer import ThreadingMixIn
import threading
#import argparse
import re
import cgi

__version__ = '0.1.0'

# Events handlers
_on_buzzer_command = None

class HTTPRequestHandler(BaseHTTPRequestHandler):
	def do_POST(self):
		print "POST", self.path
		if None != re.search('/buzzer', self.path):
			ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
			if ctype == 'application/json':
				length = int(self.headers.getheader('content-length'))
				#data = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
				data = self.rfile.read(length)
				print "got json", data
				if callable(_on_buzzer_command):
						_on_buzzer_command(data)
			else:
				data = {}
			self.send_response(200)
			self.end_headers()
		else:
			self.send_response(403)
			self.send_header('Content-Type', 'application/json')
			self.end_headers()
		return
 
	def do_GET(self):
		self.send_response(200)
		self.send_header('Content-Type', 'text/plain')
		self.end_headers()
		self.wfile.write('PiBuzz Audio Server')
		return
 
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
	allow_reuse_address = True
	
	def shutdown(self):
		self.socket.close()
		HTTPServer.shutdown(self)
 
class SimpleHttpServer():
	def __init__(self, ip, port):
		self.server = ThreadedHTTPServer((ip,port), HTTPRequestHandler)

	def start(self):
		self.server_thread = threading.Thread(target=self.server.serve_forever)
		self.server_thread.daemon = True
		self.server_thread.start()
 
	def waitForThread(self):
		self.server_thread.join()
 
	def stop(self):
		self.server.shutdown()
		self.waitForThread()
  
def buzzer_command():
    '''Bind an action webhooks
    '''
    def register(handler):
        global _on_buzzer_command
        _on_buzzer_command = handler

    return register


def start(ip,port):
	'''Initial setup of the server
	'''
	global server
	server = SimpleHttpServer(ip, port)
	print 'HTTP Server Starting...........'
	server.start()
	#server.waitForThread()
	print 'HTTP Server Started'

def _exit():
	'''Shut off server
	'''
	global server
	print 'HTTP Server Shutting down'
	server.stop()

atexit.register(_exit)

