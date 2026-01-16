import { Request, Response, NextFunction } from "express";
import { Logger } from "x-zen";


export function LoggerMiddleware(req: Request, res: Response, next: NextFunction) {
    const logger = new Logger({ context: LoggerMiddleware.name, timestamp: true });

    const { url, ip, method } = req;
    const { statusCode } = res;

    logger.log(`${url} - ${ip} - ${method} - ${statusCode}`);
    next();
}