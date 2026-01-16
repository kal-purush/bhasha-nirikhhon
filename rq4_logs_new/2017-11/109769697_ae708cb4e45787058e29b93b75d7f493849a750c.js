var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);


app.get('/', function(req, res){
  res.sendFile('index.html', { root: __dirname });
});

io.on('connection',function(socket){
  var addedUser = false;
  console.log('a user connected');

    socket.on('chat message', function(data) {
    console.log('message: ' + data.message);
    console.log('name: ' + data.name);
    io.emit('chat message', data);
    });
    
    
    
    
    socket.on('disconnect',function(){
        console.log('the user disconnected');
    });
    
    
});

http.listen(8080, function(){
  console.log('listening on *:8080');
  console.log('http://chat-application-ryanlb22.c9users.io:8080');
});