import log from "loglevel";
import prefix from "loglevel-plugin-prefix";

export type LogLevelValue = 0 | 1 | 2 | 3 | 4 | 5;

function parseLogLevelFromEnv(): LogLevelValue {
    const logLevel = process.env.APP_LOG_LEVEL;

    if (logLevel !== undefined) {
        switch (logLevel.toUpperCase()) {
            case "TRACE":
                return log.levels.TRACE;
            case "DEBUG":
                return log.levels.DEBUG;
            case "INFO":
                return log.levels.INFO;
            case "WARN":
                return log.levels.WARN;
            case "ERROR":
                return log.levels.ERROR;
            case "SILENT":
                return log.levels.SILENT;
            default:
                console.error(`Invalid log level: ${logLevel}`);
                return log.levels.INFO;
        }
    }
    return log.levels.INFO;

}

export const GLOBAL_LOG_LEVEL: LogLevelValue = parseLogLevelFromEnv();


prefix.reg(log);
prefix.apply(log, {
  format(level, name, timestamp) {
    return `${timestamp} ${level.toUpperCase()}:`;
  },
  timestampFormatter(date) {
    return date.toISOString();
  },
});

const currentLogLevel: LogLevelValue = GLOBAL_LOG_LEVEL;
log.setLevel(currentLogLevel);

const logger = log;


export default logger;