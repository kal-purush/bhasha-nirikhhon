import { NextFunction, Request, Response } from "express";
import jwt from "jsonwebtoken";

interface JwtPayload {
    sub_id: string;
}

export const authenticateUser = (req: Request, res: Response, next: NextFunction) => {
    const authHeader = req.headers['authorization'];
    // The && below checks for falsy values (null, undefined, 0, etc.) and returns itself if the value is falsy
    const token = authHeader && authHeader.split(" ")[1];

    if (!token) {
        res.status(401).json({ message: "Unauthenticated" });
        return;
    }

    jwt.verify(
        token,
        process.env.JWT_SECRET as string,
        (error, decoded) => {
            if (error) {
                res.status(403).json({ message: "Token invalid or expired." });
                return;
            }
            
            // Attach decoded data to request object
            req.decoded_user = decoded as JwtPayload;
            next();
        }
    );
}