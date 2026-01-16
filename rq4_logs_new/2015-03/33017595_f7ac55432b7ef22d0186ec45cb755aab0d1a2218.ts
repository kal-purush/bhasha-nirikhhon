/// <reference path="typings/node/node.d.ts"/>

import http = require('http');
http.createServer((req,res) => {
  res.writeHead(200, {'Content-Type': 'text/html'});
  res.end('Hello from Azure running node ersion: ' + process.version + '</br>');
}).listen(process.env.PORT || 3000);