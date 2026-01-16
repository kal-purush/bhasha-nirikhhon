import tornado.web
import tornado.httpserver
import tornado.ioloop
import tornado.options
from uuid import uuid4
import os.path

from tornado.options import define,options
define("port",default=8000,help="run on the given port",type=int)

class ShoppingCart(object):
    totalInventory=10
    callbacks=[]
    carts={}

    def register(self,callback):
        self.callbacks.append(callback)

    def moveItemToCart(self,session):
        if session in self.carts:
            return

        self.carts[session]=True
        self.notifyCallbacks()

    def removeItemFromCart(self,session):
        if session not in self.cart:
            return

        del(self.carts[session])
        self.notifyCallBacks()
    def notifyCallBacks(self):
        for c in self.callbacks:
            self.callbackHelper(c)

        self.callbacks=[]

    def callbackHelper(self,callback):
        callback(self.getInventory())

    def getInventoryCount(self):
        return self.totalInventory-len(self.carts)

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("username")

class DetailHandler(BaseHandler):


    @tornado.web.authenticated
    def get(self):
        if not self.current_user:
            self.redirect('/auth/login')
        session=uuid4()
        count=self.application.shoppingCart.getInventoryCount()
        self.render("index.html",session=session,count=count,user=self.current_user)


#    @tornado.web.authenticated
#    def get(self):
#        self.render('test.html',user=self.current_user)

class CartHandler(tornado.web.RequestHandler):
    def post(self):
        action=self.get_argument('action')
        session=self.get_argument('session')

        if not session:
            self.set_status(400)
            return

        if action=='add':
            self.application.ShoppingCart.moveItemToCart(session)
        elif action=='remove':
            self.application.shoppingCart.removItemFromCart(session)
        else:
            self.set_status(400)

class StatusHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        self.application.shoppingCart.register(self.async_callback(self.on_message))

    def on_message(self,count):
        self.write("{'inventoryCount':'%d'}"% count)
        self.finish()


class LoginHandler(BaseHandler):
    def get(self):
        self.render('auth/login.html')

    def post(self):
        self.set_secure_cookie('username',self.get_argument("username"))
        self.redirect("/")

class LogoutHandler(BaseHandler):
    def get(self):
        if (self.get_argument("logout",None)):
            self.clear_cookie("username")
            self.redirect("/")

class Application(tornado.web.Application):
    def __init__(self):
        self.shoppingCart=ShoppingCart()

        handlers=[
         (r'/',DetailHandler),
        (r'/cart',CartHandler),
        (r'/cat/status',StatusHandler),
        (r'/login',LoginHandler),
        (r'/logout',LogoutHandler)
        ]

        settings={
        'template_path':os.path.join(os.path.dirname(__file__),'templates'),
        'static_path':os.path.join(os.path.dirname(__file__),'static'),
        'cookie_secret':'Qyx4hrMaRj61//YRwMMpCSFsjR73okM4phRhbVISLsg=',
        'xsrf_cookies':True,
        'login_url':"/login"
        }

#    totalInventorynado.web.Application.__init__(self, handlers, **settings)
        tornado.web.Application.__init__(self, handlers, **settings)

if __name__=='__main__':
    tornado.options.parse_command_line()

    app=Application()
    server=tornado.httpserver.HTTPServer(app)
    server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
