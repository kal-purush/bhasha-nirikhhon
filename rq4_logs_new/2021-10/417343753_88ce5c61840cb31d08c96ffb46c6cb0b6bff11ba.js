
var express = require('express');
var app = express();
var http = require('http');
var server = http.createServer(app).listen(80);

app.get('/dksl', function (req, res) {
res.sendfile("dksl.html");
});

console.log("server is running...")