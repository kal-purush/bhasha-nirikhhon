import threading
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from bottle import default_app, hook, request, response, route
from resources.lib import cache

import xbmcgui
import xbmc

app_name = "Cakeview/0.7"


class SilentWSGIRequestHandler(WSGIRequestHandler):
    """Custom WSGI Request Handler with logging disabled"""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args, **kwargs):
        """Disable log messages"""
        pass


class ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    """Multi-threaded WSGI server"""

    allow_reuse_address = True
    daemon_threads = True
    timeout = 1


@hook("before_request")
def set_server_header():
    response.set_header("Server", app_name)


@route("/")
def index():
    response.content_type = "text/plain"
    return f"Welcome to {app_name}"


@route("/post_captcha", method="POST")
def post_captcha():
    response.content_type = "text/plain"
    url = request.params.get("url")
    cookie = request.params.get("cookie")
    user_agent = request.params.get("user_agent")
    if url and cookie and user_agent:
        xbmcgui.Window(10000).setProperty("captcha_cookie", cookie)
        xbmcgui.Window(10000).setProperty("captcha_user_agent", user_agent)
        xbmcgui.Window(10000).setProperty("captcha_url", url)
        xbmcgui.Window(10000).setProperty("captcha_closed", "1")
        request.app.stop()
        return "OK"
    else:
        return "Missing parameters"


@route("/url")
def get_url():
    response.content_type = "text/plain"
    return request.app.url


class WebServerThread(threading.Thread):
    def __init__(self, httpd: WSGIServer):
        threading.Thread.__init__(self)
        self.web_killed = threading.Event()
        self.httpd = httpd
        xbmc.log("[%s] Web server thread initialized" % app_name, xbmc.LOGERROR)

    def run(self):
        while not self.web_killed.is_set():
            self.httpd.handle_request()

    def stop(self):
        self.web_killed.set()


def get_cf_cookie():
    xbmc.log("[%s] Getting cookie" % app_name, xbmc.LOGERROR)
    xbmc.log(
        "[%s] Cookie: %s"
        % (app_name, xbmcgui.Window(10000).getProperty("captcha_cookie")),
        xbmc.LOGERROR,
    )
    return xbmcgui.Window(10000).getProperty("captcha_cookie")


def get_cf_user_agent():
    xbmc.log("[%s] Getting user agent" % app_name, xbmc.LOGERROR)
    xbmc.log(
        "[%s] User agent: %s"
        % (app_name, xbmcgui.Window(10000).getProperty("captcha_user_agent")),
        xbmc.LOGERROR,
    )
    return xbmcgui.Window(10000).getProperty("captcha_user_agent")


def get_cf_url():
    return xbmcgui.Window(10000).getProperty("captcha_url")


def start_serving(url):
    app = default_app()
    app.url = url
    try:
        httpd = make_server(
            "0.0.0.0",
            34332,
            app,
            server_class=ThreadedWSGIServer,
            handler_class=SilentWSGIRequestHandler,
        )
    except OSError as e:
        if e.errno == 98:
            xbmc.log(
                "[%s] Port %s is already in use." % (app_name, 34332),
                xbmc.LOGERROR,
            )
            dialog = xbmcgui.Dialog()
            dialog.notification(
                "Port Error",
                "Port %s is already in use. Please close any other instances of Kodi and try again."
                % 34332,
                xbmcgui.NOTIFICATION_ERROR,
                10000,
            )
            return
        raise
    web_thread = WebServerThread(httpd)
    app.stop = web_thread.stop
    xbmc.log("[%s] Starting web server" % app_name, xbmc.LOGERROR)
    web_thread.start()
    xbmc.log("[%s] Web server started" % app_name, xbmc.LOGERROR)
    ip_adress = xbmc.getIPAddress()
    xbmc.log("[%s] IP address: %s" % (app_name, ip_adress), xbmc.LOGERROR)
    busy_message = xbmcgui.DialogProgress()
    busy_message.create(
        "Cloudflare Captcha",
        "Please give the following IP to the mobile app: %s" % ip_adress,
    )
    busy_message.update(0)
    while (
        not xbmcgui.Window(10000).getProperty("captcha_closed") == "1"
        and not busy_message.iscanceled()
    ):
        xbmc.sleep(100)
    busy_message.close()
    web_thread.stop()
    web_thread.join()
    if xbmcgui.Window(10000).getProperty("captcha_closed") == "1":
        cache.drop_table("captcha")
        cache.get(get_cf_cookie, 1, table="captcha")
        cache.get(get_cf_user_agent, 1, table="captcha")
        cache.get(get_cf_url, 1, table="captcha")
    xbmcgui.Window(10000).setProperty("captcha_closed", "0")