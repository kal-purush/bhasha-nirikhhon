// this is a middleware function that validate the user cookies and validate users
import { Request, Response, NextFunction } from "express";
import cookieParser from "cookie-parser";
import jwt from 'jsonwebtoken';

function token_required(req: Request, res: Response, next: NextFunction, permissions_list: string[]) {
    // Get the cookies from the client
    const authToken = req.cookies['authToken'];

    // if the token is not found in cookies
    if (!authToken) {
        return res.status(401).json({
            message : `Unauthorized. Token not found in the request header.`
        })
    }

    // decode the token to get the user email and permissions
    try {
        const decodedToken: any = jwt.verify(authToken, process.env.JWT_SECRET_KEY);
        for (const permissions of permissions_list) {
            const isPresent: boolean = decodedToken.permissions.includes(permissions);
            if(!isPresent) {
                return res.status(403).json({ message: `Unauthorized access!`});
            }
        }
        // User is authenticated, continue to the next middleware or route handler
        next();
    } catch(err) {
        return res.status(400).json({ message: `Token is invalid! `});
    }
}

export default token_required;