/// <reference path="../typings/tsd.d.ts" />

'use strict';

import * as express from 'express';
import * as bodyParser from 'body-parser';
import * as compress from 'compression';
import * as favicon from 'serve-favicon';
import * as logger from 'morgan';
import * as cookieParser from 'cookie-parser';
import * as ejs from 'ejs';
import * as _ from 'underscore';
import * as mvc from './app/routes';
import * as api from './app/routes/api';
import * as errorHandler from './app/utils/errorHandler';
import * as four0four from './app/utils/404';
import * as secret from './app/auth/secret';
import * as db from './app/db';

// initialize app
var app = express();
var port = process.env.PORT || 4000;
var environment = process.env.NODE_ENV;

app.use(favicon(__dirname + '/public/favicon.ico'));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(compress());
app.use(logger('dev'));
app.use(errorHandler.init);
app.use(cookieParser(secret.secretToken));

db.init();

// register api routes
app.use(api.init());

// register MVC routes
app.use(mvc.init());

console.log('About to crank up node');
console.log('PORT=' + port);
console.log('NODE_ENV=' + environment);

switch (environment) {
    case 'release':
        console.log('** RELEASE **');
        app.use(express.static('./build/release/public'));
        // Any deep link calls should return index.html
        app.use('/*', express.static('./build/release/public/index.html'));
        break;
    default:
        console.log('** DEBUG **');
        app.use(express.static('./build/debug/public'));
        // Any deep link calls should return index.html
        app.use('/*', express.static('./build/debug/public/index.html'));
        break;
}

app.listen(port, () => {
    console.log('Express server listening on port ' + port);
    console.log('env = ' + app.get('env') +
        '\n__dirname = ' + __dirname +
        '\nprocess.cwd = ' + process.cwd());
});