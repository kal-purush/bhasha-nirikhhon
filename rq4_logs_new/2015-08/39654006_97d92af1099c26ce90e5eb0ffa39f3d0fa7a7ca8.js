var port = process.env.PORT || 8080;

var http = require('http');
http.createServer(function (req, res) {
  res.end('Hi There');
}).listen(port, '0.0.0.0');
console.log('Server running at http://0.0.0.0:'+port+'/');