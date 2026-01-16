var express = require('express');

var app = express();

var port = 3000;

app.use(express.static('src'));

app.get("/", function(req, res){
	res.send("Wouldn't you like to be a pepper, too?");
});

app.get("/games", function(req, res){
	res.send("Wouldn't you like to play games?");
});

//Listen on the port 
app.listen(port, function(err){
	console.log("Server is running on port " + port);
});