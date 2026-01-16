import json
from tornado.httpserver import HTTPServer
from  tornado.web import URLSpec
import tornado.ioloop
#import tornado.options
import tornado.web
from tornado.options import define, options

from orders import Book, Buy, Sell

define("port", default=3000, help="run on the given port", type=int)
Book()


class BookHandler(tornado.web.RequestHandler):

    def get(self):
        ret = json.dumps(Book().orders())
        self.write(ret)


class OrderHandler(tornado.web.RequestHandler):

    def post(self, **kwargs):
        order = None
        body = json.loads(self.request.body)
        if self.request.uri == "/buy":
            order = Buy(**body)
        if self.request.uri == "/sell":
            order = Sell(**body)
        fills = Book().match(order)
        self.write(json.dumps(fills))


def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        URLSpec(r"/book", BookHandler, name="book"),
        URLSpec(r"/buy", OrderHandler, name="buy"),
        URLSpec(r"/sell", OrderHandler, name="sell"),
    ])
    http_server = HTTPServer(application)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()