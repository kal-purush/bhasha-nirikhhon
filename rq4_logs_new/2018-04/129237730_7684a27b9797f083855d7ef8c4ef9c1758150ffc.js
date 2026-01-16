let AWS = require('aws-sdk');
exports.handler = function(event, context, callback) {
	let message = event.message;

	console.log(message);

	callback(null,'Successfully executed');
}