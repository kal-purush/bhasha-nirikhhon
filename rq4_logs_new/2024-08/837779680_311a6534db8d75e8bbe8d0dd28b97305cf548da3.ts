import winston, { Logger, format } from 'winston';

const { json, cli, timestamp, combine } = format;

const logger: Logger = winston.createLogger({
  // for production, you might not need for example 'silly' level to be used,
  // so you specify the minimum level to be logged in you environment variables.
  level: process.env.LOG_LEVEL || 'info',
  format: combine(timestamp(), cli()), // time in UTC standard
  transports: [
    new winston.transports.Console()],
});

const fileLogger: Logger = winston.createLogger({
  // for production, you might not need for example 'silly' level to be used,
  // so you specify the minimum level to be logged in you environment variables.
  level: process.env.LOG_LEVEL || 'info',
  format: combine(timestamp(), json()),
  transports: [
    new (winston.transports.File)({
      filename: './logs/.log',
    }),
    new (winston.transports.File)({
      filename: './logs/errors.log',
      level: 'error',
    }),
  ],
  exceptionHandlers: [
    new winston.transports.File({ filename: './logs/exceptions.log' }),
  ],
  rejectionHandlers: [
    new winston.transports.File({ filename: './logs/rejections.log' }),
  ],
});

export { logger, fileLogger };