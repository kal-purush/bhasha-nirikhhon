var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);

app.use(express.static(__dirname + '/public'));

io.on('connection', function (socket) {
    //console.log('a user connected, socket is: ', socket);

    socket.on('disconnect', function () {
        console.log('user disconnected');
    });

    socket.on('voitexMessage', function (data) {
        // we tell the client to execute 'new message'
        console.log('The recieved message is: ', data);
    });
});


server.listen(3000, function () {
    console.log('Example app listening on port 3000!');
});