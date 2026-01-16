import express = require('express');
import nconf = require('nconf');
import humanGenerator = require('./myModule');

import exphb = require('express-handlebars');
 
var app = express();

var human = new humanGenerator({ arms: true, legs: false, head: true });

app.get('/', function (request, response) {
    response.send(200, human.arms);
});

var server = app.listen(process.env.port || 3000 , function () {
	var port = server.address().port;
	
	console.log('App listening on port %s', port);
});