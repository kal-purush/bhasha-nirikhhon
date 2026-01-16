import json
from typing import Callable
from tornado.web import Application, RequestHandler, stream_request_body

from tornado_asgi.adapter import TornadoASGIAdapter
from starlette.testclient import TestClient


def build_test_client(
    handler: Callable[[RequestHandler], None], method: str = "GET", path: str = "/"
):
    handler_type = type("TestHandler", (RequestHandler,), {method.lower(): handler})
    tornado_app = Application([(path, handler_type)])
    return TestClient(TornadoASGIAdapter(tornado_app))


def test_write_body():
    def handler(self):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"message": "hello world"}))

    client = build_test_client(handler)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "hello world"}
    assert response.headers["Content-Type"] == "application/json"


def test_write_status():
    def handler(self):
        self.set_status(404)
        self.write("Not Found")

    client = build_test_client(handler)
    response = client.get("/")
    assert response.status_code == 404
    assert response.text == "Not Found"


def test_params():
    params = {"name": "world", "weather": "晴空万里"}

    def handler(self: RequestHandler):
        self.write(json.dumps({k: self.get_query_argument(k) for k in params}))

    client = build_test_client(handler)
    response = client.get("/", params=params)
    assert response.status_code == 200
    assert response.json() == params


def test_post_body():
    def handler(self):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps({"body": self.request.body.decode("utf-8")}))

    client = build_test_client(handler, method="POST")
    response = client.post("/", content="你好世界")
    assert response.status_code == 200
    assert response.json() == {"body": "你好世界"}


def test_stream_body():
    body = b"0" * 1024 * 10

    length = 0

    @stream_request_body
    class Handler(RequestHandler):
        def post(self):
            self.write("OK")

        def data_received(self, chunk: bytes) -> None:
            nonlocal length
            length += len(chunk)

    client = TestClient(TornadoASGIAdapter(Application([("/", Handler)])))
    response = client.post("/", content=body)
    assert response.status_code == 200
    assert response.text == "OK"
    assert length == len(body)


def test_path():
    def handler(self):
        self.write("OK")

    client = build_test_client(handler, path="/path/to/resource")
    response = client.get("/path/to/resource")
    assert response.status_code == 200
    assert response.text == "OK"


def test_request_headers():
    def handler(self: RequestHandler):
        self.write(self.request.headers.get("X-Request-ID"))

    client = build_test_client(handler)
    response = client.get("/", headers={"X-Request-ID": "123"})
    assert response.status_code == 200
    assert response.text == "123"