// central point for passing middleware to the server
import Koa from 'koa';
const bodyParser = require('koa-bodyparser')();
const compress = require('koa-compress')();

const cors = require('@koa/cors')(/* Add your cors option */);
const helmet = require('koa-helmet')(/* Add your security option */);
const logger = require('koa-logger')();

import errorHandler from './middleware/error.middleware';
import toysRouter from './api/toys/toy.routes';
import usersRouter from './api/users/user.routes';
import testRouter from './api/test';

const server = new Koa();

// Add here only development middlewares
if (process.env.NODE_ENV) {
  server.use(logger);
}

// Pass to the server instance middlewares
server.use(errorHandler).use(helmet).use(compress).use(cors).use(bodyParser);

// Apply to the server the api routers
server.use(toysRouter.routes());
server.use(usersRouter.routes());
server.use(testRouter.routes());

export default server;