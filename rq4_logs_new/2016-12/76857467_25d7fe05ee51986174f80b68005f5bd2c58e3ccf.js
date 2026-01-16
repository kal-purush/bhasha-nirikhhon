// load the things we need
var express = require('express');  
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.set('view engine', 'ejs');  
// app.set('port', (process.env.PORT || 3000));

app.get('/', function(req, res){
 	res.render('pages/index');
});

// var user_count=0;

//當新的使用者進入聊天室
io.on('connection',function(socket){
	//新user
 	socket.on('add user',function(msg){
 		socket.username=msg;
 		console.log("new user:"+msg+"logged.");
 		io.emit('add user',{
 			username:socket.username
 		});
 	});
 	//監聽新訊息事件
 	socket.on('chat message',function(msg){
 		console.log(socket.username+":"+msg);
 		//發佈新訊息
 		io.emit('chat message',{
 			username:socket.username,
 			msg:msg
 		});
 	});
 	//離開聊天室
 	socket.on('disconnect',function(){
 		console.log(socket.username+"left.");
 		io.emit('user left',{
 			username:socket.username
 		});
 	});
});

http.listen(process.env.PORT || 3000, function() {  
  console.log('Listening on port 3000');  
});