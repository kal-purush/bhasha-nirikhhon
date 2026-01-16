# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 06:34:46 2023

@author: marie
"""

import http.server
 
PORT = 8888
server_address = ("", PORT)

server = http.server.HTTPServer
handler = http.server.CGIHTTPRequestHandler
handler.cgi_directories = ["/"]
print("Serveur actif sur le port :", PORT)

httpd = server(server_address, handler)
httpd.serve_forever()
