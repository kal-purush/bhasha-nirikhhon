var host = '193.1.28.19';
var port = 8181;
var securePort = 443;
var path = require('path');
var fs = require('fs');
var express = require('express');
var morgan = require('morgan');
var http = require('http');
var https = require('https');



 var options = {
      key: fs.readFileSync('fake-keys/key.pem', 'utf8'),
      cert: fs.readFileSync('fake-keys/server.crt', 'utf8')
   };

/*
var options = {
    key: fs.readFileSync('fake-keys/privatekey.pem'),
    cert: fs.readFileSync('fake-keys/certificate.pem')
};

*/

var app = express();
app.use(morgan('dev'));

//app.use(express.static(__dirname + '/public'));
app.use(express.static(path.join(__dirname + '/public')));

/*

try{
    http.createServer(app).listen(port, host, function(){
    console.log("HTTP server listening at " + host + ":" + port)
    });   
}
catch ( e ) {
    console.log("Error: " + e );
}

*/

try{
    https.createServer(options, app).listen(securePort, host, function(){
        console.log("HTTP server listening at " + host + ":" + securePort)
    });    
}
catch ( e ) {
    console.log("Error: " + e );
}

