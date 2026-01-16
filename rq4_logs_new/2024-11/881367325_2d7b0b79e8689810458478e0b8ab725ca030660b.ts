import { Request, Response } from "express";


export const AuthMiddleware = (request: Request, response: Response, next: Function) => {
    try {
        const headers = request.headers
        const auth = headers.authorization
        console.log(auth);
        
        const apiKey: string = auth ? auth : ''
        if (apiKey === 'Bearer HolaMundo123') {
            next()
        } else {
            response.status(401).send({
                ok: false,
                message: 'No Auth'
            }
            )
        }
    } catch (error) {
        response.status(401).send({
            ok: false,
            message: 'No Auth'
        }
        )
    }

}