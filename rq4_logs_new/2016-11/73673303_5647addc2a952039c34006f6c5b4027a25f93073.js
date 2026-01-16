var ShortLogger = require('./ShortLogger');
var BadLogger = require('./BadLogger');


function LoggerAdapter(loggerObj) {
    if (!loggerObj) {
        throw Error('Parameter [loggerObj] is not defined.');
    }
    console.log('[LoggerAdapter] is using Logger with name: ' + loggerObj.getName());
    var CONSTANTS = {
        DEBUG: 'DEBUG',
        WARNING: 'WARNING',
        INFORMATION: 'INFORMATION',
        ERROR: 'ERROR',
        BAD_LOGGER: 'BadLogger',
        SHORT_LOGGER: 'ShortLogger'
    };

    var information = function information(message) {
        switch (loggerObj.getType()) {
            case CONSTANTS.BAD_LOGGER:
                logWithBadLogger(CONSTANTS.INFORMATION, message);
                break;
            case CONSTANTS.SHORT_LOGGER:
                logWithShortLogger(CONSTANTS.INFORMATION, message);
                break;
            default:
                throw Error('No implementation for Logger: ' + loggerObj.toString());
        }
    };

    var debug = function debug(message) {
        switch (loggerObj.getType()) {
            case CONSTANTS.BAD_LOGGER:
                logWithBadLogger(CONSTANTS.DEBUG, message);
                break;
            case CONSTANTS.SHORT_LOGGER:
                logWithShortLogger(CONSTANTS.DEBUG, message);
                break;
            default:
                throw Error('No implementation for Logger: ' + loggerObj.toString());
        }
    };

    var warning = function warning(message) {
        switch (loggerObj.getType()) {
            case CONSTANTS.BAD_LOGGER:
                logWithBadLogger(CONSTANTS.WARNING, message);
                break;
            case CONSTANTS.SHORT_LOGGER:
                logWithShortLogger(CONSTANTS.WARNING, message);
                break;
            default:
                throw Error('No implementation for Logger: ' + loggerObj.toString());
        }
    };

    var error = function error(message) {
        switch (loggerObj.getType()) {
            case CONSTANTS.BAD_LOGGER:
                logWithBadLogger(CONSTANTS.ERROR, message);
                break;
            case CONSTANTS.SHORT_LOGGER:
                logWithShortLogger(CONSTANTS.ERROR, message);
                break;
            default:
                throw Error('No implementation for Logger: ' + loggerObj.toString());
        }
    };

    var logWithBadLogger = function logWithBadLogger(logEntryType, message) {
        switch (logEntryType) {
            case CONSTANTS.DEBUG:
                loggerObj.debg(message);
                break;
            case CONSTANTS.INFORMATION:
                loggerObj.information(message);
                break;
            case CONSTANTS.WARNING:
                loggerObj.w(message);
                break;
            case CONSTANTS.ERROR:
                loggerObj.err(message);
                break;
            default:
                throw Error('Log Entry type NOT AVAILABLE, please use one of the following:' + JSON.stringify(CONSTANTS));
        }
    };

    var logWithShortLogger = function logWithShortLogger(logEntryType, message) {
        switch (logEntryType) {
            case CONSTANTS.DEBUG:
                loggerObj.d(message);
                break;
            case CONSTANTS.INFORMATION:
                loggerObj.i(message);
                break;
            case CONSTANTS.WARNING:
                loggerObj.w(message);
                break;
            case CONSTANTS.ERROR:
                loggerObj.e(message);
                break;
            default:
                throw Error('Log Entry type NOT AVAILABLE, please use one of the following:' + JSON.stringify(CONSTANTS));
        }
    }

    return {
        debug: debug,
        information: information,
        warning: warning,
        error: error
    }
}

module.exports = {
    LoggerAdapter: LoggerAdapter
}