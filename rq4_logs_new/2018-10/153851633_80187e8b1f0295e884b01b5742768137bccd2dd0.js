
const PORT = 8080; // Change value here to use a different port
const express = require('express');
const app = express();
const http =  require('http'); 
const server = http.Server(app)
	
// Serve the files in the app folder - if you want to serve files in a different folder, change the name here
app.use(express.static('app'));

server.listen(PORT, function() {
	console.log(`Listening at port: ${PORT}`);
});