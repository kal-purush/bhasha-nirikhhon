# !/usr/bin/env python

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import sys

# HTTPRequestHandler class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):

    # GET
    def do_GET(self):
        # Send response status code
        self.send_response(200)

        # Send headers
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        with open('/app/data/data.json', 'r') as myjson:
            message = myjson.readlines()
        # Write content as utf-8 data
        self.wfile.write(bytes(str(message[0]), 'utf8'))
        return


def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', 80)
    httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
    sys.stderr.write('running server...')
    httpd.serve_forever()


run()