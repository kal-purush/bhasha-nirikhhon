import dotenv from 'dotenv';
import { createLogger, format, Logger, transports } from 'winston';

const { combine, timestamp, printf } = format;
const myFormat = printf(({ level, label, message, timestamp }) => {
  return `${timestamp} ${level}: [${label}] ${message}`;
});

dotenv.config();

export default (): Logger => {
  return createLogger({
    transports: [
      new transports.File({
        filename: 'error.log',
        level: process.env.LOG_LEVEL_FILE,
        handleExceptions: true,
        format: combine(timestamp(), myFormat),
        maxsize: 5242880, // 5MB
        maxFiles: 5,
      }),
      new transports.Console({
        level: process.env.LOG_LEVEL_CONSOLE,
        handleExceptions: true,
        format: combine(timestamp(), format.colorize(), myFormat),
      }),
    ],
  });
};