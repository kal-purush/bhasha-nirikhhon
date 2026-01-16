#!/usr/bin/env python3
"""
Main application entry point for the Flask HTTP server.

This module implements a simple HTTP server using Flask that responds with
'Hello, World!' for any HTTP request. It is functionally equivalent to the
original Node.js implementation but written in Python using Flask.
"""

from flask import Flask, Response

# Define server configuration constants
hostname = '127.0.0.1'  # Localhost IP address
port = 3000            # Port to listen on

# Initialize Flask application
app = Flask(__name__)


# Define route handler for all paths and HTTP methods
@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'])
def hello(path):
    """
    Handle all HTTP requests and return 'Hello, World!' response.
    
    This function serves as a catch-all handler for any HTTP path and method,
    returning the same response for all requests, matching the behavior of
    the original Node.js server.
    
    Args:
        path (str): The URL path requested (unused but required for routing)
        
    Returns:
        Response: Flask Response object with 'Hello, World!' text, content type
                 of 'text/plain', and HTTP status code 200 (OK)
    """
    return Response('Hello, World!\n', mimetype='text/plain', status=200)


# Run the server if this file is executed directly
if __name__ == '__main__':
    # Log server startup information
    print(f'Server running at http://{hostname}:{port}/')
    
    # Start the Flask development server
    # Note: In production, a proper WSGI server would be used instead
    app.run(host=hostname, port=port)