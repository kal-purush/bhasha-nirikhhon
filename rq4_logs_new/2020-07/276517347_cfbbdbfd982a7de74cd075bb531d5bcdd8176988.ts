import { Context } from "https://deno.land/x/oak/mod.ts";
import { UNAUTHORIZED } from '../utils/resp-status-utils.ts'
import { verifyToken } from '../utils/jwt-util.ts'

export const authMiddleware = async (ctx: Context, next: any) => {

    const headers: Headers = ctx.request.headers;
    const authorization = headers.get("Authorization");

    // check if user actually have auth in headers
    if(!authorization){
        ctx.response.status = UNAUTHORIZED
        ctx.response.body = {
            message: "Authorization not found"
        }
        return
    }

    const jwtToken = authorization.split(' ')[1]
    // check if user has a token bearer
    if(jwtToken == ""){
        ctx.response.status = UNAUTHORIZED
        ctx.response.body = {
            message: "Token not found"
        }
        return
    }

    // check if user has a valid token
    if(verifyToken(jwtToken)){
        await next()
        return
    }

    ctx.response.status = UNAUTHORIZED
    ctx.response.body = {
        message: "You have an invalid token or your token has expired."
    }
}