const http = require('http');
const os = require('os');

console.log("Ying's web server starting...");

var handler = function(request, response) {  
    console.log("Received request from " + request.connection.remoteAddress);
      response.writeHead(200);  
      response.end("Frank, You've hit " + os.hostname() + "\n");
};

var www = http.createServer(handler);

var portNumber = 8080;
www.listen(portNumber);

console.log("Ying's web server listening on ..." + portNumber);