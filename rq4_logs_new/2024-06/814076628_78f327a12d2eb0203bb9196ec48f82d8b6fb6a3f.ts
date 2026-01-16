import { verify } from 'hono/jwt';
import { Context, Next } from 'hono';
import { PrismaClient } from '@prisma/client/edge';
import { withAccelerate } from '@prisma/extension-accelerate';

async function authMiddleware(c : Context, next : Next) {
    const authHeader = c.req.header('Authorization');
    if(!authHeader) {
        return c.json({
            msg: "Missing token"
        }, 411);
    }

    try {
        const jwtToken = authHeader;

        const decodedJwt = await verify(jwtToken, c.env.JWT_KEY);
        if(!decodedJwt) {
            return c.json({
                error: "Unauthorized"
            }, 411);
        }
        c.set('userId', decodedJwt.id);
        await next();

    } catch (err) {
        console.log(err);
        return c.json({
            msg: "Failed to authenticate"
        }, 411);
    }
}

async function initPrismaClient(c : Context, next : Next){
    try {
        const prisma = new PrismaClient({
            datasourceUrl: c.env.DATABASE_URL,
        }).$extends(withAccelerate());

        c.set('prisma', prisma);
        await next();

    } catch (err) {
        console.log(err);
        return c.text("Unable to initialize prisma", 400);
    }
}

export {
    authMiddleware,
    initPrismaClient
}