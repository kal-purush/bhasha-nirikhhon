var express = require("express");
var app = express();
var port = process.env.PORT || 3000;
app.listen(port);
app.use(express.static("./client/static"));


var Server = require("http").Server;

var server = Server(app);
console.log("Server running on port " + port);