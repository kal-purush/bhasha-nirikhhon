from http.server import BaseHTTPRequestHandler, HTTPServer,os,time
import socketserver
from sys import argv
class Myserver(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        try:
            if self.headers.get('If-Modified-Since', None):
                filename = self.path.strip("/")
                if os.path.isfile(filename):
                    statbuf = os.stat(filename)
                    a = statbuf.st_mtime
                    # a = time.strptime(time.ctime(os.path.getmtime(filename)), "%a %b %d %H:%M:%S %Y")
                    b = self.headers.get('If-Modified-Since', None)
                    if a < float(b):
                        self.send_response(304)
                        self.end_headers()
                        return

            temp = self.path.strip('/')
            print(temp)
            temp = open(temp,'r')
            self._set_response()
            self.wfile.write(temp.read().encode())
            return 
        except:
            self._set_response()
            print("Error Serving the Request\n")
            self.wfile.write("Error serving the Request".encode())
            return 

    
    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length']) 
            post_data = self.rfile.read(content_length) 
            print("POSTED DATA:",post_data.decode())
            self._set_response()
            self.wfile.write("POST request sent\n".encode())
        except:
            self._set_response()
            print("Error Serving the Request\n")
            self.wfile.write("Error serving the Request".encode())
            return 

if __name__ == '__main__':
    PORT = 20103
    if(argv[1] != None):
        PORT = int(argv[1])
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    s = socketserver.ThreadingTCPServer(("", PORT),Myserver)
    s.allow_reuse_address = True
    print ("Serving on port", PORT)
    s.serve_forever()