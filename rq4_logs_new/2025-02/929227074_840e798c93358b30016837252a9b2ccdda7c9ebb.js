// Create web server
// Create a simple web server with Node.js.

// Load the http module to create an http server.
var http = require('http');
var fs = require('fs');

// Configure our HTTP server to respond with Hello World to all requests.
var server = http.createServer(function (request, response) {
    console.log('request ', request.url);

    if (request.url === '/') {
        response.writeHead(200, { 'Content-Type': 'text/html' });
        fs.createReadStream('./index.html').pipe(response);
    } else if (request.url === '/api') {
        response.writeHead(200, { 'Content-Type': 'application/json' });
        var obj = {
            firstname: 'John',
            lastname: 'Doe'
        };
        response.end(JSON.stringify(obj));
    } else {
        response.writeHead(404);
        response.end();
    }

});

// Listen on port 8000, IP defaults to