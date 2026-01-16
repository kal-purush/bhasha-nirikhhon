from __future__ import print_function
from submitter import *
import SimpleHTTPServer
import SocketServer

PORT = 8000

if __name__ == '__main__':
	print("Start kafka consumer...")
	#submitter = BasicSubmitter("batch.etl.test")
	#submitter = ThreadPoolSubmitter("batch.etl.test", max_workers=3)
	submitter = FSMSubmitter("batch.etl.test", need_tables={'A', 'B', 'C'})
	submitter.start()

	# == start a server ==
	#input("Wait for message...")
	Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
	httpd = SocketServer.TCPServer(("", PORT), Handler)
	print("serving client app at port: {}".format(PORT))
	httpd.serve_forever()