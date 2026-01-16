import sys

if sys.implementation.name == "cpython":
    import asyncio
    import json
    import os

if sys.implementation.name == "micropython":
    import uasyncio as asyncio
    import ujson as json
    import uos as os


class HttpRequest:
    """A basic http request
    """
    def __init__(self, method: str, url: str, version: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Initialize a http request. You shouldn't have to do it by yourself.

        Args:
            method (str): The HTTP method (GET, POST, DELETE, ...)
            url (str): The requested URL
            version (str): The HTTP version
            reader (asyncio.StreamReader): an async stream reader
            writer (asyncio.StreamWriter): an async stream writer
        """
        self.method = method
        self.url = url
        self.version = version
        self.reader = reader
        self.writer = writer
        self.headers = {}

    async def _process_headers(self, ignore_headers:bool=False):
        while self.reader is not None:
            header_line = await self.reader.readline()
            header_items = header_line.decode("ascii").split(": ", 1)
            if len(header_items) != 2:
                return
            if not ignore_headers:
                self.headers[header_items[0]] = header_items[1].strip()
    
    async def _process_body(self):
        self.raw_body = ""
        if "Content-Length" in self.headers:
            if "Content-Type" in self.headers:
                try:            
                    if self.headers["Content-Type"].startswith("text/html"):
                        self.raw_body = await self.reader.read(int(self.headers["Content-Length"]))
                        self.body = self.raw_body.decode("utf-8")
                    elif self.headers["Content-Type"].startswith("application/json"):
                        self.raw_body = await self.reader.read(int(self.headers["Content-Length"]))
                        self.body = json.loads(self.raw_body.decode("utf-8"))
                    else:
                        self.raw_body = await self.reader.read(int(self.headers["Content-Length"]))   
                except:
                    print("Could not parse Request Body")
                    raise ValueError("Could not parse Request Body")
    
    async def write(self, buf):
        if type(buf) is str:
            self.writer.write(buf.encode('utf8'))
        else:
            self.writer.write(buf)
        #await self.writer.awrite(buf)
        await self.writer.drain()

    async def close(self):
        try:
            # Seems to be required for some Browsers
            await self.reader.read(-1)
        finally:
            self.writer.close()

class HttpResponse:
    def __init__(self, request: HttpRequest, data: any, code: int=200, headers=None, default_headers: bool=True):
        self.request = request
        self.data = data
        self.code = code
        if headers is None:
            self.headers = {}
        else:
            self.headers = headers
        _default_headers = {
            "Connection": "close",
            "Access-Control-Allow-Origin": "*"
            }
        
        if isinstance(self.data, str):
            _default_headers["Content-Length"] = len(self.data)
            _default_headers["Content-Type"] = "text/html"
        elif isinstance(self.data, dict):
            _default_headers["Content-Length"] = len(json.dumps(self.data))
            _default_headers["Content-Type"] = "application/json"
        elif isinstance(self.data, list):
            _default_headers["Content-Length"] = len(json.dumps(self.data))
            _default_headers["Content-Type"] = "application/json"
        elif isinstance(self.data, tuple):
            if self.data[0] == "file":
                _default_headers["Content-Length"] = os.stat(self.data[2])[6]-2
                _default_headers["Content-Type"] = self.data[1]

        if default_headers:
            for header in _default_headers:
                if header not in self.headers:
                    self.headers[header] = _default_headers[header]   

    async def _send(self):
        if self.code == 200:
            await self.request.write(f"HTTP/1.1 200 OK\r\n")
        elif self.code == 400:
            await self.request.write(f"HTTP/1.1 400 Bad Request\r\n")
        elif self.code == 404:
            await self.request.write(f"HTTP/1.1 404 Not Found\r\n")
        elif self.code == 405:
            await self.request.write(f"HTTP/1.1 405 Method Not Allowed\r\n")
        elif self.code == 500:
            await self.request.write(f"HTTP/1.1 500 Internal Server Error\r\n")

        for header in self.headers:
            await self.request.write(f"{header}: {self.headers[header]}\r\n")

        await self.request.write(f"\r\n")

        if isinstance(self.data, str):
            await self.request.write(self.data)
        elif isinstance(self.data, dict):
            await self.request.write(json.dumps(self.data))
        elif isinstance(self.data, list):
            await self.request.write(json.dumps(self.data))
        elif isinstance(self.data, tuple):
            if self.data[0] == "file":
                with open(self.data[2], encoding="utf-8") as f:
                    while True:
                        s = f.read(1024)
                        if s != '':
                            await self.request.write(s)
                        else:
                            break

class HttpError(HttpResponse):
    def __init__(self, request: HttpRequest, code: int, message: str=None):
        if message is not None:
            super().__init__(request, message, code)
        else:
            if code == 400:
                super().__init__(request, "The request (header or body) is malformatted", code)        
            elif code == 404:
                super().__init__(request, "The requested URL was not found on this server.", code)       
            elif code == 405:
                super().__init__(request, "Method Not Allowed", code)      
            elif code == 500:
                super().__init__(request, "Internal Server Error", code)

class HttpServer:
    """A HTTP Server that proviedes basic functionality and runs on micropython.
    """
    def __init__(self, port: int=80, address: str='0.0.0.0'):
        """Initializes the server.

        Args:
            port (int, optional): The port, on which the server listens. Defaults to 80.
            address (str, optional): The IP address of the server. Defaults to '0.0.0.0'.
        """
        self.port = port
        self.address = address
        self.routes = {}
        self.async_routes = {}
        self.server = None

    def route(self, route: str, methods: tuple=("GET", )):
        """Route decorator"""
        def decorator(func):
            self.routes[route] = (methods, func)
            return func
        return decorator

    def async_route(self, route: str, methods: tuple=("GET", )):
        """Route decorator for async routes"""
        def decorator(func):
            self.async_routes[route] = (methods, func)
            return func
        return decorator
    
    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        req_line = await reader.readline()
        
        # request line should be like
        # GET /index.html HTTP/1.1
        req_items = req_line.decode("ascii").split(" ")
        if len(req_items) != 3:
            return
        request = HttpRequest(req_items[0], req_items[1], req_items[2], reader, writer)
        
        print(f"Request: {request.method:.10} {request.url:.30} -> ", end="")
        
        failed = False

        try:
            try:
                await request._process_headers()
            except:
                failed = True
                response = HttpError(request, 400, "Could not process header fields")
                await response._send()
                print(f"400 (Could not process header)")
            
            if not failed:
                try:
                    await request._process_body()
                except:
                    failed = True
                    response = HttpError(request, 400, "Could not process body")
                    await response._send()
                    print(f"400 (Could not process body)")
            if not failed:
                if request.url in self.routes:
                    if request.method in self.routes[request.url][0]:
                        try:
                            response = self.routes[request.url][1](request)
                        except:
                            response = HttpError(request, 500)     
                    else:
                        response = HttpError(request, 405, f"Method {request.method} not allowed")            
                elif request.url in self.async_routes:
                    if request.method in self.async_routes[request.url][0]:
                        try:
                            response = await self.async_routes[request.url][1](request)
                        except:
                            response = HttpError(request, 500)                    
                    else:
                        response = HttpError(request, 405, f"Method {request.method} not allowed")
                else:
                    response = HttpError(request, 404, f"URL {request.url} not found")
                
                await response._send()
                print(f"{response.code} {str(response.data):.100}")
        finally:
            await request.close()

    async def run(self):
        if self.server is None:
            self.server = await asyncio.start_server(self.handle, self.address, self.port)

    def stop(self):
        self.server.close()