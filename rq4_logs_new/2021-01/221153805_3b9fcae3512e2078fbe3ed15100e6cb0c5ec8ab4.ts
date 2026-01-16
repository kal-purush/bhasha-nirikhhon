import { Request } from 'express';
import { error, debug, info, LOG_LEVEL, warning } from '@navikt/familie-logging';

export const envVar = (navn: string, påkrevd = true, defaultValue?: string): string => {
    const envVariable = process.env[navn];
    if (!envVariable && påkrevd && !defaultValue) {
        error(`Mangler påkrevd miljøvariabel '${navn}'`);
        process.exit(1);
    }
    if (!envVariable && defaultValue) {
        return defaultValue;
    } else {
        return envVariable as string;
    }
};

const prefix = (req: Request) => {
    return `${
        req.session && req.session.user ? `${req.session.user.displayName} -` : 'ugyldig sesjon -'
    } ${req.method} - ${req.originalUrl}`;
};

export const logRequest = (req: Request, message: string, level: LOG_LEVEL) => {
    const melding = `${prefix(req)}: ${message}`;
    switch (level) {
        case LOG_LEVEL.DEBUG:
            debug(melding);
            break;
        case LOG_LEVEL.INFO:
            info(melding);
            break;
        case LOG_LEVEL.WARNING:
            warning(melding);
            break;
        case LOG_LEVEL.ERROR:
            error(melding);
            break;
        default:
            info(melding);
    }
};