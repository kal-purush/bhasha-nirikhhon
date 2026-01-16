import { ApolloServer } from "apollo-server";
const typeDefs = require("../../../../schema.graphql");
import resolvers from "./resolvers";
import { Gateways } from "../../types";
import { TypeOrmDatabaseGateway } from "../../gateways/TypeOrmDatabaseGateway";
import { Context } from "./types";

type MakeApolloServerOptions = {
  dev?: boolean;
};

export function makeApolloServer(options?: MakeApolloServerOptions) {
  const dev = Boolean(options && options.dev);
  const gateways: Gateways = {
    databaseGateway: new TypeOrmDatabaseGateway({ dev })
  };
  return new ApolloServer({
    typeDefs,
    resolvers,
    ...(dev ? devOptions : {}),
    context: async ({ req }): Promise<Context> => {
      return {
        gateways,
        authInfo: {
          userId: "1"
        }
      };
    }
  });
}

const devOptions = {
  formatError: (error: any) => {
    console.log(error);
    return error;
  },
  formatResponse: (response: any) => {
    console.log(response);
    return response;
  },
  playground: true
};