namespace Blend.logger {

    export interface LogContext {
        [s: string]: any;
    }

    export interface LoggerInterface {

        log(type: string, message: string, context?: LogContext);

        warn(message: string, context?: LogContext);

        error(message: string, context?: LogContext);

        info(message: string, context?: LogContext);

        debug(message: string, context?: LogContext);
    }

}