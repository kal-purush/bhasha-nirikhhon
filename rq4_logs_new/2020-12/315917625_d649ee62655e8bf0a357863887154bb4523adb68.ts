import { createLogger, format, transports } from 'winston';
import { env } from 'config/environment';

const logger = createLogger({
    level: env.NODE_LOGGING_LEVEL,
    format: format.combine(
        format.timestamp({
            format: 'YYYY-MM-DD HH:mm:ss',
        }),
        format.colorize({ all: true }),
        format.errors({ stack: true }),
        format.simple(),
    ),
    exitOnError: false,
    transports: [
        new transports.Console({
            level: env.NODE_LOGGING_LEVEL,
            silent: env.NODE_ENV === 'test',
            format: format.combine(format.colorize(), format.simple()),
        }),
    ],
});

export { logger };

// const logger = createLogger({
//     level: 'info',
//     format: format.combine(
//         format.timestamp({
//             format: 'YYYY-MM-DD HH:mm:ss',
//         }),
//         format.errors({ stack: true }),
//         format.splat(),
//         format.json(),
//     ),
//     defaultMeta: { service: 'Dav-express' },
//     transports: [
//         //
//         // - Write to all logs with level `info` and below to `log-combined.log`.
//         // - Write all logs error (and below) to `log-error.log`.
//         //
//         new transports.File({ filename: 'log-combined.log', level: 'error' }),
//         new transports.File({ filename: 'log-error.log' }),
//     ],
// });

// if (process.env.NODE_ENV !== 'production') {
//     logger.add(
//         new transports.Console({
//             format: format.combine(format.colorize(), format.simple()),
//         }),
//     );
// }