"""wsgi entry point

This module serves as the entry point for our application. It defines the behaviour
of the Gunicorn server with our application.
"""

from app import app
if __name__ == '__main__':
    app.run_server()