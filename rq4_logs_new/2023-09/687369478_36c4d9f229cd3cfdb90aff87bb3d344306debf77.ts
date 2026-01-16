import createError from "http-errors";
import dotenv from  "dotenv";
import express, { Express, Request, Response } from 'express';
import http from "http";
import path from "path";
import cookieParser from "cookie-parser";
import logger from "morgan";
import mongoose from "mongoose";
import { ROUTE_QUESTIONS, router as questionsRouter } from "./routes/questions";
import { CODE_BAD_REQUEST, CODE_INTERNAL_SERVER_ERROR } from "./utility";

/**
 * Read environment variables
 */ 
function readEnvironmentVariables() {
  dotenv.config();
}

/**
 * Set up database
 */
function initialiseDatabase() {
  // The URI should look like "mongodb+srv://admin:<password>@<clusterxx.yyyyyy>.mongodb.net/test?retryWrites=true&w=majority"
  const mongoDbUri = process.env.MONGO_URI;
  if (!mongoDbUri) {
    console.log("MongoDB atlas URI not found in environment");
  } else {
    mongoose.connect(mongoDbUri);
  }
}

/**
 * Gets port as a string, number or false.
 */
function getPort() {
  const rawPort = process.env.PORT || '3000';
  const port = parseInt(rawPort, 10);
  if (isNaN(port)) {
    // named pipe
    return rawPort;
  }
  if (port >= 0) {
    // port number
    return port;
  }
  return false;
}

// Set up server
function initialiseExpressApp(port: string | number | false) {
  const app: Express = express();
  app.set('port', port);

  app.use(logger('dev'));
  app.use(express.json());
  app.use(express.urlencoded({ extended: false }));
  app.use(cookieParser());
  app.use(express.static(path.join(__dirname, 'public')));

  app.use(ROUTE_QUESTIONS, questionsRouter);

  // catch 400 and forward to error handler
  app.use(function(req: Request, res: Response, next) {
    next(createError(CODE_BAD_REQUEST));
  });

  // error handler
  app.use(function(err: any, req: Request, res: Response) {
    // set locals, only providing error in development
    res.locals.message = err.message;
    res.locals.error = req.app.get('env') === 'development' ? err : {};

    // respond with 400
    res.status(err.status || CODE_INTERNAL_SERVER_ERROR).end();
  });

  return app;
}

/**
 * Set up HTTP server.
 */
function initialiseServer(app: Express, port: string | number | false) {

  /**
   * Event listener for HTTP server "error" event.
   */
  function onError(error: any) {
    if (error.syscall !== 'listen') {
      throw error;
    }

    const bind = typeof port === 'string'
      ? 'Pipe ' + port
      : 'Port ' + port;

    // handle specific listen errors with friendly messages
    switch (error.code) {
      case 'EACCES':
        console.error(bind + ' requires elevated privileges');
        process.exit(1);
      case 'EADDRINUSE':
        console.error(bind + ' is already in use');
        process.exit(1);
      default:
        throw error;
    }
  }

  /**
   * Event listener for HTTP server "listening" event.
   */
  function onListening() {
    const addr = server.address();
    if (!addr) {
      console.error('No address found on server');
    } else {
      const bind = typeof addr === 'string'
        ? 'pipe ' + addr
        : 'port ' + addr.port;
      console.log('Listening on ' + bind);
    }
  }

  const server = http.createServer(app);
  // Listen on provided port, on all network interfaces.
  server.listen(port);
  server.on('error', onError);
  server.on('listening', onListening);
  return server;
}

/**
 * Runs the server.
 */
function main() {
  readEnvironmentVariables();
  initialiseDatabase();
  const port = getPort();
  const app = initialiseExpressApp(port);
  initialiseServer(app, port);
}

main();