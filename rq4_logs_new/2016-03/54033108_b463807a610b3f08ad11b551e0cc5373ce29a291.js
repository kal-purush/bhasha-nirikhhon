var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.get('/', function(req,res){
    res.sendFile(__dirname + '/client.html');
});

io.on('connection', function(socket){
    socket.on('chat message', function(msg){
        console.log("received " + msg);
    });
});

http.listen(process.env.PORT, process.env.IP);