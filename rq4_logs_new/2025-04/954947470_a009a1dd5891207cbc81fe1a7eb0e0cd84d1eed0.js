"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.loggingHandler = loggingHandler;
const logger_1 = __importDefault(require("../logger"));
function loggingHandler(req, resp, next) {
    logger_1.default.info(`Method : [${req.method}] - URL : [${req.url}] - IP : [${req.socket.remoteAddress}]`);
    resp.on('finish', () => {
        logger_1.default.info(`STATUS : [${resp.statusCode}]`);
    });
    next();
}