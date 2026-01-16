"""
This script runs the application using a development server.
It contains the definition of routes and views for the application.
"""

from flask import Flask
app = Flask(__name__,template_folder='C:\\Users\mohdyusuf\Desktop\proj\WordWebAddIn1\WordWebAddIn1Web',static_folder='C:\\Users\mohdyusuf\Desktop\proj\WordWebAddIn1\WordWebAddIn1Web')

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app


from route import *

if __name__ == '__main__':
#    app.run(host='0.0.0.0', port=80)
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5555'))
    except ValueError:
        PORT = 5555
    app.run(HOST, PORT)