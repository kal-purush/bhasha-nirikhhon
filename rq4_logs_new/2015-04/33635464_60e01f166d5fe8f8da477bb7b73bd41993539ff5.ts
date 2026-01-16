///<reference path='defs/node/node.d.ts' />
///<reference path='defs/express/express.d.ts' />

import express = require('express');
import http = require('http');
import path = require('path');

var app = express();
var server = require('http').Server(app);
var io = require('socket.io')(server); 

// all environments
app.use(express.static('public')); 

server.listen(3000, function () {
	var host = server.address().address;
	var port = server.address().port;
	console.log('Listening at http://%s:%s', host, port); 
});