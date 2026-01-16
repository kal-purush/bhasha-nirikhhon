import { Request, Response, NextFunction } from 'express';
import { Prisma } from '@prisma/client';

const handleError = async (err: Prisma.PrismaClientKnownRequestError | string, req: Request, res: Response, next: NextFunction) => {

    if (typeof (err) === 'string') return res.status(401).json(err);
    console.log("----------error middleware----------------");
    console.log(err.code);
    let responseMsg;
    switch (err.code) {
        case 'P2002':
        case 'P2025':
            responseMsg = err.message.split('\n')[8].trim();
            break;
        default:
            responseMsg = "Something Went Wrong!"
            break;
    }
    res.status(401).json(responseMsg);
}

export default handleError