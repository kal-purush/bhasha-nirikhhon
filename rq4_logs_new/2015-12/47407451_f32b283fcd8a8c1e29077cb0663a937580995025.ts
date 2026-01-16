import fs = require('fs')

var inputFile = './app.js';
var outputFile = './tasker-ping-response.js';

var input = fs.readFileSync(inputFile, 'utf8');
var output = 'exports = {};' +
	'\n' + input + 
	'\n' + 'var responder = new PingResponder(global, 1200);' +
	'\n' + 'var smsbody = responder.ComposeResponse();'
	
fs.writeFileSync(outputFile, output, {encoding: 'utf8'});
console.log('Done writing file')