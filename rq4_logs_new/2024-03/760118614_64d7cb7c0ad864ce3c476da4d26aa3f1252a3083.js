const { PrismaClient } = require("@prisma/client");
const expressAsyncHandler = require("express-async-handler");
const { verifyCodedToken } = require("./jwt");

const prisma = new PrismaClient({
    errorFormat: 'minimal'
})

const loggedInMidddleware = expressAsyncHandler(async (req, res, next) => {
    let token;
    if (token = req.cookies['LIT']) {
        try {
            const user = await prisma.user.findUnique({
                where: {
                    id: verifyCodedToken(token)?.id
                }
            })

            if (!user) {
                req.user = null
                next()
            }

            req.user = user
            next()  
        } catch (error) {
            req.user = null
            next()
        } 
    }
    else {
        req.user = null
        next()
    }
})


module.exports = { loggedInMidddleware }