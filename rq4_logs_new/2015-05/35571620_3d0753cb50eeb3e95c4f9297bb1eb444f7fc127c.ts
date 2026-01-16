///<reference path="./typings/node/node.d.ts"/>
///<reference path="./typings/express/express.d.ts"/>

import express = require('express');

var app = express();

app.use('/web', express.static(__dirname + '/public'));

app.get('/', function(req, res) {
    console.log("req:" + (JSON.stringify(req.headers)) );
    res.send('req:' + req );
});

console.log( "Starting on " + process.env.PORT);
app.listen(process.env.PORT);
console.log( "Listening on " + process.env.PORT);