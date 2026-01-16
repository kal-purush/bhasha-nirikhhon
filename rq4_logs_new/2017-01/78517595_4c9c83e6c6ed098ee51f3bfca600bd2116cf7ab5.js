var fs = require('fs');
var http = require('http');
var https = require('https');

// ssl certificates
var privateKey = fs.readFileSync('ssl/25994996-localhost.key','utf8');
var certificate = fs.readFileSync('ssl/25994996-localhost.cert','utf8');
var credentials = {key: privateKey, cert: certificate};
var express = require('express');
var app = express();

// allow cross origin requests
var cors = require('cors');
app.use(cors());

var httpServer = http.createServer(app);
var httpsServer = https.createServer(credentials,app);

// store stuff
storage = {};

// parse json requests
var bodyParser = require('body-parser');
app.use(bodyParser.json());

// api endpoints
app.get('/setstock',function(req,res){
	res.sendStatus(200);
	storage.value = parseFloat(req.query.ask);
	process.stdout.clearLine();
	process.stdout.cursorTo(0);
	process.stdout.write("current value: " + storage.value);
});

// lauch server
httpServer.listen(3000);
httpsServer.listen(3001);