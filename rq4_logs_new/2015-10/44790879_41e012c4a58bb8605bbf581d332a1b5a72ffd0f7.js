var express = require('express');
var app = express();

var MARKER_FILE = "/var/marker.txt";

app.get('/', function (req, res) {
  res.sendfile(MARKER_FILE);
});

var server = app.listen(80, function () {
  var host = server.address().address;
  var port = server.address().port;
  console.log('Using markerfile: %s', MARKER_FILE);
  console.log('every http get request succeeds if this file exists, fails otherwise');
  console.log('http filecheck listening on http://%s:%s', host, port);
});