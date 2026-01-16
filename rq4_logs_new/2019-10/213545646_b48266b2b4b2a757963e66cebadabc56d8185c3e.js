// using the express framework  to create an application

var express = require('express');
var path = require('path');



var app = express();
var http = require('http').createServer(app);

var io = require('socket.io')(http);
var port = process.env.PORT | 3000 

//Middleware to get css accessible to clients
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', function(req, res){
    res.sendFile(__dirname + '/form.html');
  });

  io.on('connection', function(socket){
    socket.on('chat message', function(msg){
      //console.log('message: ' + msg);
      io.emit('new message', msg);
    });
  });

 

io.on('disconnect', function(socket){
    console.log('A user just disconnected')

});

  http.listen(3000, function(){
    console.log('listening on *:3000');
  });