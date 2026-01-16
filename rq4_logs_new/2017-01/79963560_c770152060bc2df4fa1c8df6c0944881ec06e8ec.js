/**
 * Logger Utility
 * @see - https://github.com/winstonjs/winston
 * @see - http://tostring.it/2014/06/23/advanced-logging-with-nodejs/
 *
 * Log Levels:
 * See example log functions at the bottom of this file
 * emerg
 * alert
 * crit
 * error
 * warning
 * notice
 * info
 * debug
 */

const express = require('express');
const morgan = require('morgan');
const winston = require('winston');
const fs = require('fs');

const app = express();
const logDir = './logs';

// Logger placeholder function
let logger = {
  debug: console.log
};

// Setup Winston Logging, similar to error logs and access logs
if( app.get('logLevel') !== false && app.get('logLevel') !== 'none' ) {

  // Set Winston Configs
  winston.emitErrs = true;

  // Create the log directory if it does not exist
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
  }

  const tsFormat = () => (new Date()).toLocaleTimeString();

  logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({
        level: (app.get('env') === 'development' ? 'debug' : 'info'),
        timestamp: tsFormat,
        colorize: true,
        handleExceptions: true,
        json: false
      }),
      new (winston.transports.File)({
        filename: logDir + '/error_log.log',
        name:'file.error',
        level: 'error',
        maxsize: 1024000,
        maxFiles: 10,
        tailable: true,
        handleExceptions: false,
        json: true,
        colorize: false
      })
    ],
    exitOnError: false
  });

  // Use syslog type errors, instead of default winston errors
  logger.setLevels(winston.config.syslog.levels);

  // On logger errors
  logger.on('error', function (err) {
    if( app.get('env') === 'production' ) {
      // @todo - Email Sysadmin
    }
  });
}

module.exports = logger;
module.exports.stream = {
  write: function(message, encoding){
    if(logger.info) {
      logger.info(message);
    } else {
      logger.debug(message);
    }
  }
};

// Test Logs
// logger.debug("There's no place like home");
// logger.info("There's no place like home");
// logger.notice("There's no place like home");
// logger.warning("There's no place like home");
// logger.error("There's no place like home");
// logger.crit("There's no place like home");
// logger.alert("There's no place like home");
// logger.emerg("There's no place like home");