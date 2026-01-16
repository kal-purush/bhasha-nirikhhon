var express = require('express');
var app = express();
var harmony = require('harmonyhubjs-client');
var harmonyip = '192.168.1.170';

app.get('/', function (req, res) {
	res.send('Hello World!');
});

//Start TV
app.get('/WatchTV', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting Watch TV...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Stop TV
app.get('/StopTV', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        console.log("Turning Off...");
	        harmonyClient.turnOff();
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Start Apple TV
app.get('/AppleTV', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting AppleTV...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Start Firestick
app.get('/Firestick', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting Firestick...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Start Xbox One
app.get('/Xbox', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting Xbox One...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Start WiiU
app.get('/WiiU', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting Wii U...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Start Chromecast
app.get('/Chromecast', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        var accid = 'XXXXXXXX';
	        console.log("Starting Chromecast...");
	        harmonyClient.startActivity(accid);
	        harmonyClient.end();
	});
	res.sendStatus(200);
});

//Get list of activity ids
app.get('/Activities', function (req, res) {
	harmony(harmonyip)
	.then(function(harmonyClient) {
	        harmonyClient.getActivities()
	        .then(function(activities) {
	                activities.some(function(activity) {
	                        console.log(activity.label + '(' + activity.id + ')');
	                });
	                harmonyClient.end();
	        });
	});
	res.send('Check console for activity list');
});


var server = app.listen(8088, function () {
	var port = server.address().port;

	console.log('Server running on port %s', port);
});