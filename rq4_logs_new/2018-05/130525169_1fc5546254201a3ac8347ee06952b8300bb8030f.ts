const config = require('config');
const winston = require('winston');

class Logger {
    public logger;

    constructor() {
        this.logger = winston.createLogger({
            transports: [
                new winston.transports.Console({ level: config.get('logger').level })
            ],
            format: winston.format.combine(
                winston.format.colorize(),
                winston.format.timestamp(),
                winston.format.align(),
                winston.format.printf((info) => {
                    const {
                        timestamp, level, message, ...args
                    } = info;

                    const ts = timestamp.slice(0, 19).replace('T', ' ');
                    return `${ts} [${level}]: ${message} ${Object.keys(args).length ? JSON.stringify(args, null, 2) : ''}`;
                }),
            )
        });
    }
}
export default new Logger().logger;