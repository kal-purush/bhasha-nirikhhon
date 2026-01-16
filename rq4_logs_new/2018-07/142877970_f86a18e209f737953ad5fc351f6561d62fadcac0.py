from flask import Flask
from flask_restplus import Resource, Api


# welcome to flask: http://flask.pocoo.org/
# working with sqlalchemy & swagger:
# http://michal.karzynski.pl/blog/2016/06/19/building-beautiful-restful-apis-using-flask-swagger-ui-flask-restplus/
application = Flask(__name__)
api = Api(application)


@api.route("/hello")                   # Create a URL route to this resource
class HelloWorld(Resource):            # Create a RESTful resource
    def get(self):                     # Create GET endpoint
        return {'hello': 'world'}


def main():
    application.debug = True
    application.run()


if __name__ == "__main__":
    main()