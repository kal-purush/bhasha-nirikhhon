var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io').listen(server);
var request = require('request');
var bodyParser= require('body-parser');
var mc = require('mongodb').MongoClient;
var fs = require('fs');

app.use('/', express.static(__dirname + '/public'));

var token =0;
var usernames = {};
var pairCount = 0, id, clientsno, pgmstart=0,varCounter;
var scores = {};
var db;

mc.connect('mongodb://admin:admin@ds229435.mlab.com:29435/realtimequiz', function (err,database){
	if (err) return console.log(err);
	db = database;
	server.listen(4444, function(){
		console.log("Listening to 4444");
		console.log("Connection Established !");
	});	
});

//Route
app.use(bodyParser.urlencoded({extended: true}));

app.get('/',function (req,res){
	res.sendFile(__dirname + '/index.html');
});

app.get('/admin', function (req,res){
	if (token == 1){
		res.sendFile(__dirname + '/dashboard.html');
	}
	else{
		res.sendFile(__dirname + '/admin.html');
	}
});

app.get('/dashboard', function(req,res){
	if (token == 1){
		res.sendFile(__dirname + '/dashboard.html');
	}
	else{
		res.send("<script>alert('Anda Harus Login Terlebih Dahulu');window.location='http://localhost:4444/admin';</script>");
	}
});

app.get('/logout', function(req,res){
    		token=0;
    		res.sendFile(__dirname + '/admin.html');
});

app.post('/dashboard', function (req,res){
	var query = { name: req.body.name };
	db.collection('user').findOne(query,function(err,results){

		if (err) {return console.log(err);}
    	if (results) {
        // we have a result
        	if(req.body.password == results.password){
        		token = 1;
				res.sendFile(__dirname + '/dashboard.html');
			}
			else{
				var string = 'Password Salah';
				res.send("<script>alert('Password Salah');window.location='http://localhost:4444/admin';</script>");
			}
    	} 
    	else {
        // we don't
        	res.send("<script>alert('Username tidak terdaftar');window.location='http://localhost:4444/admin';</script>");
    	}
	});
});

io.sockets.on('connection', function(socket){
	console.log("New Client Arrived!");

	socket.on('addClient', function(username, kode){
		socket.username = username;
		socket.kode = kode;
		usernames[username] = username;
		scores[socket.username] = 0;
		varCounter = 0
		//var id = Math.round((Math.random() * 1000000));
		pairCount++;
		if(pairCount === 1 || pairCount >=3){
			id = Math.round((Math.random() * 1000000));
			socket.room = id;
			pairCount = 1;
			console.log(pairCount + " " + id);
			socket.join(id);
			pgmstart = 1;
		} else if (pairCount === 2){
        	console.log(pairCount + " " + id);
        	socket.join(id);
        	pgmstart = 2;
    	}
		
		console.log(username + " joined to "+ id);

		socket.emit('updatechat', 'SERVER', 'You are connected! <br> Waiting for other player to connect...',id);
		
		socket.broadcast.to(id).emit('updatechat', 'SERVER', username + ' has joined to this game !',id);

		
		if(pgmstart ==2){
			request.get(kode, function(error, response, body){
				if (error){
					throw res.send("<script>alert('Kode Salah atau tidak terdaftar');window.location='http://localhost:4444/';</script>");
				}
				jsoncontent = JSON.parse(body);
				io.sockets.in(id).emit('sendQuestions',jsoncontent);
				
			});
		console.log("Player2");
			//io.sockets.in(id).emit('game', "haaaaai");
		} else {
			console.log("Player 1");

		}
  
	});


	socket.on('result', function (usr,rst) {
		
				io.sockets.in(rst).emit('viewresult',usr);
				//io.in(id).emit('viewresult',usr);
				//console.log("Mark = "+usr);
				//console.log(id);	

	});

	
	socket.on('disconnect', function(){
		
		delete usernames[socket.username];
		io.sockets.emit('updateusers', usernames);
		//io.sockets.in(id).emit('updatechat', 'SERVER', socket.username + ' has disconnected',id);
		socket.leave(socket.room);
	});
});

