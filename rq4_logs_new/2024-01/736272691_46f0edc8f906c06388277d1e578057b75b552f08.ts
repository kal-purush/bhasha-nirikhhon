import { existsSync, mkdirSync, writeFileSync } from 'fs';
import path from 'path';
import winston, { Logger } from 'winston';

export let LOGGER: Logger;

export namespace LoggerHelper {
    export function setupLogger(specName?: string) {
        const reportFolderPath: string = `${process.cwd()}/logs`;
        let loggerFileName: string = 'log.log';

        try {
            if (!existsSync(reportFolderPath)) {
                mkdirSync(reportFolderPath);
            }

            if (!(specName === undefined || specName === '' || specName === null)) {
                loggerFileName = `${snakeCase(specName)}.log`;
            }

            // declare format of the logger
            const myFormat: winston.Logform.Format = winston.format.combine(
                winston.format.colorize(),
                winston.format.timestamp({ format: 'DD-MM-YYYY HH:mm:ss' }),
                winston.format.align(),
                winston.format.printf((info) => `${info.timestamp} ${info.level}: ${info.message}`),
            );

            const logFilePath = path.join(reportFolderPath, loggerFileName);
            if (!existsSync(logFilePath)) {
                writeFileSync(logFilePath, '');
            }

            // initialize the logger
            // if (LOGGER == undefined || LOGGER == null) {
            LOGGER = winston.createLogger({
                level: 'debug',
                format: myFormat,
                transports: [
                    new winston.transports.File({
                        filename: logFilePath,
                        level: 'info',
                        maxsize: 5120000,
                    }),
                    new winston.transports.Console({
                        level: 'debug',
                    }),
                ],
            });
            LOGGER.info('-----------------------------');
            LOGGER.info('LOGGER setup complete');
            LOGGER.info('-----------------------------');
            LOGGER.info(`generating log file at ${reportFolderPath}/${loggerFileName}`);
            // }
        } catch (error) {
            console.info('-----------------------------');
            console.info('LOGGER setup Failed');
            console.info('-----------------------------');
            throw new Error(error as string);
        }
    }

    export function snakeCase(str: string): string {
        return str.replace(/[^\w\d]/gi, '_').toLowerCase();
    }
}