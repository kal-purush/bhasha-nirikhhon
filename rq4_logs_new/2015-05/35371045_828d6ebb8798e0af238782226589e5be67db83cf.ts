import express = require('express');
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);

var buffer = new (require('./operations/operation-buffer'))();

import path = require('path');

app.use(express.static(path.resolve(__dirname, '../public')));

io.on('connection', function(socket) {
  io.on('operation', function(data) {
    buffer.append(data.operation, data.context);
    io.emit('operation', buffer.getLast().parameters());
  });
});

http.listen(3000, function() {
  console.log('listening on *:3000');
});