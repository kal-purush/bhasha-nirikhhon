import * as winston from 'winston';
import * as expressWinston from 'express-winston';

const configuration = {
  transports: [
    new winston.transports.Console({
      level: 'debug',
      colorize: true,
      handleExceptions: true,
      prettyPrint: true,
      humanReadableUnhandledException: true,
      timestamp: true
    }),
    new winston.transports.File({
      level: 'info',
      filename: __dirname + '/../../logs/all-logs.log',
      handleExceptions: true,
      maxsize: 5242880,
      maxFiles: 5,
      colorize: false,
      prettyPrint: true,
      tailable: true,
      timestamp: true
    })
  ],
  exitOnError: false,
};

const logger = new winston.Logger(configuration);
const expressLogger = expressWinston.logger(configuration);

export {logger, expressLogger};