import "reflect-metadata";
import express, {Express} from "express";
import {ApolloServer} from "apollo-server-express";
import {buildSchema} from "type-graphql";
import {UserResolver} from "./resolvers/user";
import { PrismaClient } from '@prisma/client'


//db connection
const prisma = new PrismaClient();

const main = async () => {
    //server
    const app: Express = express();

    const apolloServer = new ApolloServer({
        schema: await buildSchema({
            resolvers: [UserResolver],
            validate: false,
        })
    });

    await apolloServer.start();
    apolloServer.applyMiddleware({app});

    const PORT = process.env.PORT || 5000;

    app.listen(PORT, () => {
        console.log(`Running on port ${PORT}`)
    })
}

main().then(async () => {
    await prisma.$disconnect()
}).catch(async (err) => {
    console.error(err)
    await prisma.$disconnect()
    process.exit(1)
})