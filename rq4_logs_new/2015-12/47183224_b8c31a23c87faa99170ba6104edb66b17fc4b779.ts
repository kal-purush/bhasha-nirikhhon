
import * as path from 'path';
import * as express from 'express';

// import * as db from './rethinkdb';
import * as morgan from 'morgan';
import * as compression from 'compression';
import * as timeout from 'connect-timeout';

import {loggerStream, logger} from './logger';
import {errors} from './middlewares/errors';

import {Routes} from './routes';

function haltOnTimedout(req: express.Request, res: express.Response, next: Function) {
  if (!req.timedout) {
    next();
  }
}

export var app = express();
app.use(timeout('5s'));
app.use(compression());

var logFormat = 'dev';
app.use(morgan(logFormat, {
  stream: loggerStream
}));

const routes = new Routes();
app.use(routes.router);

// Default route
app.get('/', (req: express.Request, res: express.Response) => {
  res.status(200).send('OK');
});