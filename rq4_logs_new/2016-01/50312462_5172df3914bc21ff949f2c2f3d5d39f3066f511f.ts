import http = require('http');


var port:number = process.env.PORT || 8080;

http.createServer(function(req:http.ServerRequest, res:http.ServerResponse){
    res.writeHead(200, { 'Content-Type': 'text/plain'});
    res.end("Hello World\n");
}).listen(port);

