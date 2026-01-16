import { Server } from "http";
import { WebSocketServer } from "ws";

import { GraphQLError } from "graphql";
import { useServer } from "graphql-ws/lib/use/ws";
import { makeExecutableSchema } from "@graphql-tools/schema";

import { ApolloServer } from "@apollo/server";
import { ApolloServerPluginDrainHttpServer } from "@apollo/server/plugin/drainHttpServer";

import resolvers from "../resolvers/index.js";
import typeDefs from "../typeDefs/index.js";
import { getUser } from "./utils.js";

const schema = makeExecutableSchema({ typeDefs, resolvers });

export default function createApolloServer(httpServer: Server) {
    const wsServer = new WebSocketServer({
        server: httpServer,
        path: "/",
    });

    const serverCleanup = useServer(
        {
            schema,
            context: async (ctx) => {
                const user = await getUser(ctx);

                if (!user)
                    throw new GraphQLError("User is not authenticated", {
                        extensions: {
                            code: "UNAUTHENTICATED",
                            http: { status: 401 },
                        },
                    });

                return { user };
            },
            onConnect: async (ctx) => {
                const user = await getUser(ctx);
                if (!user) return false;
                console.log("Connected WebSocket");
            },
            onDisconnect(ctx, code, reason) {
                console.log("Disconnected WebSocket");
            },
        },
        wsServer
    );

    const server = new ApolloServer({
        schema,
        plugins: [
            ApolloServerPluginDrainHttpServer({ httpServer }),
            {
                async serverWillStart() {
                    return {
                        async drainServer() {
                            await serverCleanup.dispose();
                        },
                    };
                },
            },
        ],
    });

    return server;
}