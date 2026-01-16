import socket
import os

# ---------------- Handler Functions for Routes ---------------- #

def handle_home():
    return "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<h1>Home Page</h1>"

def handle_about():
    return "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<h1>About Page</h1>"

# ---------------- Handler for Static Files ---------------- #

def handle_static_file(path):
    # Remove leading slash to make relative path
    file_path = path.lstrip("/")
    if not os.path.isfile(file_path):
        return "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nFile not found"
    
    # Guess content type from file extension (very basic version)
    if file_path.endswith(".html"):
        content_type = "text/html"
    elif file_path.endswith(".css"):
        content_type = "text/css"
    elif file_path.endswith(".js"):
        content_type = "application/javascript"
    elif file_path.endswith(".png"):
        content_type = "image/png"
    elif file_path.endswith(".jpg") or file_path.endswith(".jpeg"):
        content_type = "image/jpeg"
    else:
        content_type = "application/octet-stream"
    
    # Read the file as binary
    with open(file_path, "rb") as f:
        body = f.read()
    
    # Build the HTTP response header
    header = f"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\r\n"
    response = header.encode() + body  # header is bytes, body is bytes
    return response  # Return as bytes

# ---------------- Routing Table ---------------- #

routes = {
    ('GET', '/'): handle_home,
    ('GET', '/about'): handle_about,
}

# ---------------- Main Server Code ---------------- #

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('localhost', 8080))
server_socket.listen(1)
print('Listening on http://localhost:8080 ...')

while True:
    client_socket, client_address = server_socket.accept()
    request = client_socket.recv(4096).decode(errors='replace')  # larger buffer for bigger requests
    print(request)

    # --- Parse HTTP method and path ---
    lines = request.split('\r\n')
    if lines:
        request_line = lines[0]
        parts = request_line.split()
        if len(parts) >= 2:
            method = parts[0]
            path = parts[1]
        else:
            method = None
            path = None
    else:
        method = None
        path = None

    # --- Serve static files if path starts with /static/ ---
    if method == "GET" and path and path.startswith("/static/"):
        http_response = handle_static_file(path)
        if isinstance(http_response, str):
            client_socket.sendall(http_response.encode())
        else:
            client_socket.sendall(http_response)
    else:
        handler = routes.get((method, path))
        if handler:
            http_response = handler()
            client_socket.sendall(http_response.encode())
        else:
            http_response = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nPage not found"
            client_socket.sendall(http_response.encode())

    client_socket.close()