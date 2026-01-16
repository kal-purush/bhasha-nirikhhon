/// <reference path="./typings/tsd.d.ts" />
import express = require('express');
import http = require('http');

var app = express();
app.set('port', process.env.PORT || 3000);
app.get('/', function(req: express.Request, res: express.Response) {
    res.send('Hello world!');
});

http.createServer(app).listen(app.get('port'), function() {
    console.log('Express server listening on port ' + app.get('port'));
});