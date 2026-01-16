var http = require('http');
var url = require('url');
var path = require('path');
var fs = require('fs');
var port = process.argv[2] || 8000;

http.createServer(function(request, response) {

  var uri = url.parse(request.url).pathname;
  var filename = path.join(process.cwd(), uri);

  var callback = function(exists) {
    if (!exists) {
      response.writeHead(404, {'Content-Type': 'text/plain'});
      response.write('404 Not Found\n');
      response.end();
      return;
    }

    fs.readFile(filename, 'binary', function(err, file) {
      if (err) {
        response.writeHead(500, {'Content-Type': 'text/plain'});
        response.write(err + '\n');
        response.end();
        return;
      }

      response.writeHead(200);
      response.write(file, 'binary');
      response.end();
    });
  };

  if (path.exists) {
    path.exists(filename, callback);
  } else {
    fs.exists(filename, callback);
  }
}).listen(parseInt(port, 10));

console.log('Static file server running at\n  => http://localhost:' + port + '/\nCTRL + C to shutdown');