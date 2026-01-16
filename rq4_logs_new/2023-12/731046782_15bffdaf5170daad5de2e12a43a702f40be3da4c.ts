import { NextFunction, Request , Response } from "express";
import { UnauthorizedError } from "../errorHandler/error";
import { verifyCredentials } from "../utils/jwt.helper";

export function jwtMiddleware(req: any, res: Response, next: NextFunction) {
    let token = req.cookies['x-access-token'];
    const authHeader = req.headers.authorization;
    if (!token) {
        if (!authHeader) throw new UnauthorizedError();

        const [authType, _token] = (authHeader || "").split(" ");
        if (authType.toLowerCase() !== "bearer" || !_token) throw new UnauthorizedError();

        token = _token;
    }
    const credential = verifyCredentials({ type: "accessToken", token });
    if (!credential) throw new UnauthorizedError();

    if (credential.tokenExpired) throw new UnauthorizedError();

    req.credential = credential;
    return next();
}