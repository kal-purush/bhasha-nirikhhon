import { NextFunction, Request, Response } from "express"
import jwt from "jsonwebtoken";
import dotenv from "dotenv";
const config = dotenv.config.toString();

export const checkJwt = async(req: Request, res: Response, next: NextFunction) => {
    //Destructure token
    const jwtToken = req.header("token");
    if(!jwtToken){
        return res.status(403).json("Not authorised");
    }
    //Verify token
    try {
        const verify = jwt.verify(jwtToken, config);
        req.body.student = JSON.parse(JSON.stringify(verify));
        next();
    } catch (error) {
        console.error(error.message);
        return res.status(403).json("Not authorised");
    }
}