/// <reference path="../typings/tsd.d.ts" />

'use strict';

require('es6-shim');

import * as path from 'path';
import * as http from 'http';

import * as express from 'express';
import * as compression from 'compression';
import * as bodyParser from 'body-parser';

var app = express();

// when trying to integrate socket.io, try http://socket.io/docs/#using-with-the-express-framework
//var server = new http.Server(app);
var PORT = 3000;


app.use(compression());
app.use(bodyParser.urlencoded());
app.use(bodyParser.json());

app.use(express.static(path.join(__dirname, '..', 'client')));

app.listen(PORT, function () {
    console.log('Server running on', [
        'http://localhost:',
        PORT
    ].join(''));
});

process.on('uncaughtException', function(e: Error){
    console.error('uncaught', e, e.stack);
    process.exit();
});