const express = require('express');
const http = require('http');
const url = require('url');
const WebSocket = require('ws');

const app = express();
var path = require('path');

// Define the port to run on
app.set('port', 3000);

// Listen for requests
var expressServer = app.listen(app.get('port'), function() {
  var port = expressServer.address().port;
  console.log('Magic happens on port ' + port);
});

app.use(express.static(path.join(__dirname, 'public')));


const wss = new WebSocket.Server({ port: 8080 });

var PORT = 7777;
var HOST = '10.0.0.161'; //'192.168.0.102'; //'192.168.1.147' //192.168.4.1';

var dgram = require('dgram');

var client = dgram.createSocket('udp4');

var fps = require('fps');
var ticker = fps({
    every: 100   // update every 10 frames 
});

ticker.on('data', function(framerate) {
	console.log("framerate is: %f", framerate);
})
//messageLength = (maxNumLights*3 + 3)*8;
function sendAsap(){
	//console.log(messageBuffer);
	client.send(messageBuffer, 0, messageBuffer.length, PORT, HOST, function(err, bytes) {
	    //console.log('UDP message sent to ' + HOST +':'+ PORT + ' of size: ' + messageLength);

		if (err) throw err;
		//sendAsap();
		first = true;
		//ticker.tick();
			//console.log('UDP message sent to ' + HOST +':'+ PORT);
			
		});
}
first=true;
wss.on('connection', function connection(ws) {
  ws.on('message', function incoming(message) {
	//console.log('message length: ' + message.length);
	messageBuffer = getBufferFromMessage(message);
	if (first) {first=false; sendAsap();}
  });

});

function getBufferFromMessage(theMessage){
	//console.log(message);
	/*arrayOfNumbers = theMessage.split(",").map(Number);
	var correctedArray = new Array();
	for (var i = 0;  i < arrayOfNumbers.length; i++) {
		//console.log(i);
		switch (i%3){
			case 0:
				correctedArray[i+4] = arrayOfNumbers[i]; //+1+4
				break;
			case 1:
				correctedArray[i+2] = arrayOfNumbers[i]; // -1+4
				//console.log(i);
				//console.log(i % 3);
				break;
			case 2:
				correctedArray[i+3] = arrayOfNumbers[i];
				break;
		}
	}
	return Buffer.from(correctedArray);*/
	return Buffer.from(theMessage);
}







