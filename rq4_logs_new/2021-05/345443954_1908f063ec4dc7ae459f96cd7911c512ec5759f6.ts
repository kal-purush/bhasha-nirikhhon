import { NextFunction, Response, Request } from 'express';
import jwt from 'jsonwebtoken';
import config from '../config';

export const verifyToken = (req : Request, res: Response, next: NextFunction) => {
    
    const authHeader = req.headers['authorization']
    const token = authHeader && authHeader.split(' ')[1]

    if(!token){
        return res.json({ passed: false });
    }

    jwt.verify(token, config.secret as string, (err, user) => {
        if (err) {
            return res.json({ passed: false });
        }
    })

    return res.json({ passed: true })

}
