import * as fs from "fs";
import * as path from "path";
import * as winston from "winston";
import * as pathroot from "app-root-path";

const logDir: string = path.join(pathroot.path, ".poc-chef-server-logs/");

const createLogDir = (logDir: string): void => {
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
  }
};

export const setupLogger = (): void => {
  createLogDir(logDir);
};

// errors transport
const errorTransport = new winston.transports.File({
  filename: path.join(logDir, "poc-chef-error.log"),
  level: "error",
  format: winston.format.json()
});

// debug transport
const debugTransport = new winston.transports.File({
  filename: path.join(logDir, "poc-chef-debug.log"),
  level: "debug",
  format: winston.format.json()
});

// http request transport
const requestTransport = new winston.transports.File({
  filename: path.join(logDir, "poc-chef-request.log"),
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  )
});

// create logger and log for errors and debug
export const logger: winston.Logger = winston.createLogger({
  level: "debug",
  transports: [errorTransport, debugTransport]
});

// create logger and log for http request
const requestLogger: winston.Logger = winston.createLogger({
  level: "info",
  transports: [requestTransport]
});

export const requestLogStream = {
  write: (message: string) => {
    requestLogger.info(message.trim());
  }
};