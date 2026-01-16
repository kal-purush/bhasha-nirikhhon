from flask import Flask
from flask import render_template, request, url_for, jsonify

def startup_server():
    app = Flask(__name__) # minblo server

    @app.route('/')
    def index():
        # root page
        pass


if __name__ == '__main__':
    startup_server()
else:
    import sys
    sys.stdout.write("Can not refer this scripts from other programs. \nplz run directly.")
