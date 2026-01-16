import { NextFunction, Request, Response } from "express";
import { AppError } from "../errors/appError";
import jwt from 'jsonwebtoken'


export const authenticationMiddleware = async (req: Request, res: Response, next: NextFunction) => {
    let authorization = req.headers.authorization

    if(!authorization){
        throw new AppError("Missing authentication header", 401)
    }

    const token = authorization.split(' ')[1] || authorization

    jwt.verify(
        token, 
        process.env.SECRET_KEY as string, 
        (error, decoded:any) => {
            if(!decoded){
                throw new AppError('Invalid token', 401)
            }

            req.user.id = decoded.userId
            req.user.role = decoded.userRole

            return next()
        })
        
}