import appRoot from 'app-root-path';
import winston, { format } from 'winston';

const logger = winston.createLogger({
    level: 'info',
    format: format.combine(
        format.splat(),
        format.json()
    ),
    transports: [
        new winston.transports.File({ 
            filename: `${appRoot}/logs/error.log`,
            level: 'error'
        }),
        new winston.transports.File({ 
            filename: `${appRoot}/logs/combined.log`,
        })
    ]
});

const { NODE_ENV } = process.env;
if (!NODE_ENV || (NODE_ENV.toUpperCase() !== 'PRODUCTION' && NODE_ENV.toUpperCase() !== 'TEST')) {
    logger.add(new winston.transports.Console({
        format: winston.format.simple()
    }));
}

export { logger };