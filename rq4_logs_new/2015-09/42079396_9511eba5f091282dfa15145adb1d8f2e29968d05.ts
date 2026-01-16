///<reference path='./typings/tsd.d.ts'/>
import express = require('express');
import http = require('http');
import socketIO = require('socket.io');
console.log("Hola world");

var app: express.Express = express();
var server: http.Server = http.createServer(app);
var io: SocketIO.Server = socketIO(server);

//General app stuffs
app.use(express.static(__dirname + '/public'));
app.use('/bower_components',  express.static(__dirname + '/bower_components'));
app.get('/', function(req, res) {
	res.render('index.jade');
})


//Socket.io go!
io.on('connection', function(socket: SocketIO.Socket) {
	console.log('a luzre connected!');
	socket.on('disconnect', () => {
		console.log('luzre disconnected');
	})
	
	socket.on('chatMessage', (msg)=>{
		console.log('message: \t%s', msg);
		io.emit('chatMessageUpdate', msg);
	});
})

// var server = app.listen(3000);
server.listen(3000, function() {
	console.log('listening on *:3000')
})