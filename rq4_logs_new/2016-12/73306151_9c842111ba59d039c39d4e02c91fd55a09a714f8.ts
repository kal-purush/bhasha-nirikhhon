'use strict';

import createError = require('http-errors')
import express = require('express');
import bodyParser = require('body-parser');
import {HttpError} from "http-errors";
import mongoose = require('mongoose');
let tingodb = require('tingodb');

import {router} from './routes/request-routes';

const DB_OPTION = 3;
const HOSTNAME = '127.0.0.1';
const PORT = 3001;
const DB_HOSTNAME = HOSTNAME;
const DB_PORT = 27017;

let app = express();

let db: mongoose.Connection =  mongoose.connection;
let dbLocation: any;
switch (DB_OPTION) {
    case 1:
        dbLocation = `${DB_HOSTNAME}:${DB_PORT}/homeautomation`;
        break;
    case 2:
        // not yet provided: new tingodb.Db('./data-tingodb', {});
        // break;
    default:
        dbLocation = 'mongodb://admin:hallihallo62@ds050879.mlab.com:50879/homeautomation';
}
db.once('open', () => console.log('DB is connected'));
db.on('error', console.error.bind(console, 'connection error:'));
mongoose.connect(dbLocation);

let errorHandler = function (err: Error, req: express.Request, res: express.Response, next: express.NextFunction) {
    console.log(`ErrorHandler: ${err.stack}`);
    res.status(500).send(err.message);
}

let inputLogger = function (req: express.Request, res: express.Response, next: express.NextFunction) {
    console.log(`Request: ${req.method} ${req.url}`);
    next();
}

let outputLogger = function (req: express.Request, res: express.Response, next: express.NextFunction) {
    console.log(`Response with status code: ${res.statusCode}`);
    next();
}

app.use(express.static(__dirname + '/public'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));
app.use(inputLogger);

app.use('/', function (req: express.Request, res: express.Response, next: express.NextFunction) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.setHeader('Access-Control-Allow-Methods', 'POST, GET, PATCH, DELETE');
    next();
});

app.use(router);

/*
 app.use(function(req: express.Request, res: express.Response, next: express.NextFunction) {
 var err: HttpError = createError(404, 'Not found');
 next(err);
 });
 */

app.use(outputLogger);
app.use(errorHandler);

app.listen(PORT, HOSTNAME, () => {
    console.log(`NoteServer running at http://${HOSTNAME}:${PORT}/`);
});

