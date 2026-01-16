/// <reference path="../typings/tsd.d.ts" />
"use strict";

import http = require('http');
import express = require('express');
const port = process.env.port || 3000;
const app = express();
const server = http.createServer(app);
const servestatic = require('serve-static');
const serve_dir = __dirname + '/public';
console.log('serve %s', serve_dir);
app
    .use(servestatic(serve_dir))
;

// socket.io
import socketio = require('socket.io');
const io = socketio(server);
io.on('connection', (socket) => {
    var clientAddress=socket.client.conn.remoteAddress;
    console.log('connected: %s', clientAddress);
    socket.on('disconnect', () => {
        console.log('disconnected: %s', clientAddress);
    });
});

// start
server.listen(port);
console.log('start port: %d...', port);