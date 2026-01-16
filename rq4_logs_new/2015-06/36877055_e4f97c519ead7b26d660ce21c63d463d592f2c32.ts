///<reference path='libs/express.d.ts' />
///<reference path='libs/socket.io.d.ts' />

var app = require('express')();
var chat = require('./routes/chat_route');
var http = require('http').Server(app);
var io = require('socket.io')(http);
var room_mod = require('./libs/classes/room_module');

// Uses jade to render views
app.set('views', __dirname +'/views');
app.set('view engine', 'jade');

/* Uses custom classes to hold and access information about
   users and namespaces
*/

var manager = new room_mod.roomManager();

app.get('/', function(req, res){
	res.render('index', {pageTitle: 'index'});
});

// Provides a list of rooms with information about users on startup
io.on('connection', function(socket){
	socket.emit('roomlist', manager.getRoomList());
});

/* Upon request of a chat room (/chat), the app creates a new room
   for the namespace, if the namespace doesn't exist. It then routes
   to a router that renders the chat page
*/

app.use('/chat', function(req, res, next){
	if(manager.roomCheck(io.of(req.originalUrl))){
		var room = new room_mod.Room(io.of(req.originalUrl), manager);
		manager.addRoom(room);
	}
	next();
}, chat);


http.listen(3000, function(){
	console.log("Listening at port 3000");
});
