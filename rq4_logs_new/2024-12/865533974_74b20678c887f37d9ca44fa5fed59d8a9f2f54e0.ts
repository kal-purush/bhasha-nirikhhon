import jwt from 'jsonwebtoken'
import {Request,Response,NextFunction} from 'express'
import { IAuthTokenPayload } from '../interfaces/authTokenPayload'
import { HttpStatusCodes } from '../config/HttpStatusCodes'

export const roleAuth = (allowedRoles:string[])=>{
    return (req:Request,res:Response,next:NextFunction)=>{
        const token = req.header('Authorization')?.split(' ')[1]
        if(!token){
            return res.status(HttpStatusCodes.UNAUTHORIZED).json({message:'Access denied.Token Required'})
        }
        try {
            const decoded = jwt.verify(token,process.env.JWT_SECRET as string) as IAuthTokenPayload
            if(!allowedRoles.includes(decoded.role)){
                return res.status(HttpStatusCodes.ROLE_NOT_FOUND).json({ message: 'You do not have permission to access this resource.' });
            }
            next()  
        } catch (error) {
            return res.status(HttpStatusCodes.UNAUTHORIZED).json({ message: 'Invalid or expired token.' });

        }
    }
} 
