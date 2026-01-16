const app = require('express')();
const http = require('http').Server(app);
const io = require('socket.io')(http);

// app.get('/', function(req, res) {
//   res.send('<h1>Hello World</h1>');
// });

app.get('/', function(req, res) {
  res.sendFile(__dirname + '/src/index.html');
});

io.on('connection', function(socket){
  // console.log('a user connected');
  // io.emit('user-conn', 'a user connected');
  // socket.broadcast.emit('user-conn', 'a user connected');
  //
  // socket.on('disconnect', function() {
  //   // console.log('a user disconnected');
  //   socket.broadcast.emit('user-disconn', 'a user disconnected');
  // });

  socket.on('user-join', function(username) {
    const msg = username + ' telah bergabung';
    socket.broadcast.emit('user-join', msg);
    console.log(msg);
  });

  socket.on('chat-msg', function(msgperson) {
    socket.broadcast.emit('chat-msg', msgperson);
    console.log(msgperson);
  });
});

http.listen(3000, function() {
  console.log('listening on *:3000');
});