
var http = require('http').createServer();
var io = require('socket.io')(http);

io.on('connection', function (socket) {
	
	socket.on('chat message', function (msg) {
		io.emit('chat message', msg);
		console.log('message: ' + msg);
	})

});

http.listen(3001, function () {
	console.log('listening on *:3001');
});