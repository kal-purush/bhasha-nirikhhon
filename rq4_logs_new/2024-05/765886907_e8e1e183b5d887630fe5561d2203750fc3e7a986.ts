import { expressMiddleware } from '@apollo/server/express4';

import { extractToken } from './auth';
import { mysqlConfig } from '../config';
import { DMPHubAPI } from '../datasources/dmphub-api';
import { MysqlDataSource } from '../datasources/mysqlDB';

export function attachApolloServer(apolloServer, cache, logger) {
  // expressMiddleware accepts the same arguments:
  //   an Apollo Server instance and optional configuration options
  return expressMiddleware(apolloServer, {
    context: async ({ req }) => {
      // Extract the token from the incoming request so we can pass it on to the resolvers
      const token = extractToken(req);

      return {
        // Pass the logger in so it is available to our resolvers and dataSources
        logger,
        dataSources: {
          dmphubAPIDataSource: await new DMPHubAPI({ cache, token }),
          sqlDataSource: await new MysqlDataSource({ config: mysqlConfig }),
        },
      }
    },
  });
}