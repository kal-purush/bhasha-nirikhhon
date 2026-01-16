import { Response, NextFunction} from "express";
import jwt from 'jsonwebtoken'


export const JWTVerifyMiddleware = (req: any, res: Response, Next: NextFunction) => {
    try {
        // ?. operator is really just short hand for the guard operator
        // req.headers.authorization && req.headers.authorization.split(' ').pop()
        let token = req.headers.authorization?.split(' ').pop()//turn the string Bearer token -> token
        if( token){
            req.user = jwt.verify(token, 'thisIsASecret')
        }
        Next()//no token and no req.user
        //other validation middleware will catch it
    } catch (e) {
        console.log(e);
        Next(e)
    }
}