import jwt from "express-jwt";
import jwksRsa from "jwks-rsa";
import { AUTH0_ISSUER, AUTH0_AUDIENCE } from '../utils/config'

export const checkJwt = jwt({
    secret: jwksRsa.expressJwtSecret({
        cache: true,
        rateLimit: true,
        jwksRequestsPerMinute: 5,
        strictSsl: false,
        jwksUri: `${AUTH0_ISSUER}/.well-known/openid-configuration/jwks`
    }),

    // Validate the audience and the issuer.
    audience: AUTH0_AUDIENCE,
    issuer: `${AUTH0_ISSUER}`,
    algorithms: ["RS256"]
});