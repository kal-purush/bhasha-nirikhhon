import { ApolloServer } from "apollo-server-lambda";

import typeDefs from "./schema";
import TrefleAPI from "./trefleAPI/model";
import resolvers from "./rootResolver";

const server = new ApolloServer({
  typeDefs,
  dataSources: () => ({
    trefleAPI: new TrefleAPI(),
  }),
  resolvers,
  context: async ({ event, context }) => ({
    headers: event.headers,
    functionName: context.functionName,
    event,
    context,
  }),
  playground: {
    endpoint: "/dev/graphql",
  },
  introspection: true,
  formatResponse: (response) => {
    console.log(response);
    return response;
  },
  formatError: (err) => {
    console.log(err.extensions.exception.stacktrace);
    console.log(err);
    return err;
  },
});

exports.apolloHandler = server.createHandler({
  cors: {
    origin: true,
    credentials: true,
  },
});