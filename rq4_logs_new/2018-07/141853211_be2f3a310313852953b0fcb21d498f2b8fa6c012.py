#!/usr/bin/env python
import os

from main_source import app as application

ip = os.environ.get('OPENSHIFT_PYTHON_IP', 'localhost')
port = int(os.environ.get('OPENSHIFT_PYTHON_PORT', 8051))
host_name = os.environ.get('OPENSHIFT_GEAR_DNS', 'localhost')

if __name__ == '__main__':
        from wsgiref.simple_server import make_server
        httpd = make_server('localhost', port, app = application)
        httpd.serve_forever()
        application.run(port = port, debug = True)
