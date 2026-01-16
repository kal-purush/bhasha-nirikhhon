/* tslint:disable:no-console */
import * as Path from 'path'
import * as Winston from 'winston'

// Helper method for setting up logger transports
const setLoggerTransports = () => {
    const transports = []

    const stdOutTransport = new Winston.transports.Console({
        level: 'debug',
        handleExceptions: true,
    })

    transports.push(stdOutTransport as any)
    return transports
}

Winston.format.combine(
    Winston.format.colorize(),
    Winston.format.json()
)

const logger = Winston.createLogger({
    exitOnError: false,
    transports: setLoggerTransports()
})

export default class Logger {
    public static info(msg: string, ...meta: any[]) {
        if (process.env.LOGGER_DISABLED === 'true') {
            return
        }
        logger.info(`${msg}`, ...meta)
    }

    public static trace(msg: string, ...meta: any[]) {
        if (process.env.LOGGER_DISABLED === 'true') {
            return
        }
        const err: Error = new Error()
        logger.info(`${msg}`, ...meta, 'Trace: ', err.stack)
    }

    public static warn(msg: string, ...meta: any[]) {
        if (process.env.LOGGER_DISABLED === 'true') {
            return
        }
        const err: Error = new Error()
        logger.warn(`${msg}`, ...meta)
    }

    public static error(msg: string, ...meta: any[]) {
        if (process.env.LOGGER_DISABLED === 'true') {
            return
        }
        const err: Error = new Error()
        logger.error(`${msg}`, ...meta)
    }

    public static handleError(err: Error): string {
        if (err.stack) {
            return ' with ' + err.stack
        } else {
            return ' with ' + err + 'No stack trace available for Error'
        }
    }
}

export const stream = {
    write: (message: string, encoding: any) => {
        logger.info(message)
    }
}