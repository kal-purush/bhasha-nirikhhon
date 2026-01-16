//server.js
var http = require('http');
var getInfo = require('./info.js');

http.createServer(

function onRequest(request, response) {
  response.writeHead(200, {'Content-type': 'text/plain'});
  response.write(getInfo(1));
  response.end();
}).listen(8080);