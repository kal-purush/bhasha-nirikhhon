require("coffee-script/register");
var server = require('./models/server');

server.start(3000, 'localhost');

