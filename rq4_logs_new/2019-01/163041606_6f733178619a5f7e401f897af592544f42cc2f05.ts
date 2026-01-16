import * as express from 'express';
import { ApolloEngine } from 'apollo-engine';
import { ApolloServer, mergeSchemas } from 'apollo-server-express';
import { HttpLink } from 'apollo-link-http';
import fetch from 'node-fetch';
import { introspectSchema, makeRemoteExecutableSchema } from 'graphql-tools';

interface Endpoint {
  endpoint: string;
}

interface Config {
  [name: string]: Endpoint;
}

class Gateway {
  private endpoints: string[];

  public constructor(config: Config) {
    this.endpoints = Object.values(config).map(({ endpoint }) => endpoint);
  }

  private async getSchema(endpoint: string) {
    const link = new HttpLink({ uri: endpoint, fetch } as any);
    const schema = await introspectSchema(link);
    return makeRemoteExecutableSchema({ schema, link });
  }

  public async listen({ port }: any) {
    const schemas = await Promise.all(this.endpoints.map(this.getSchema));
    const schema = mergeSchemas({ schemas });
    const server = new ApolloServer({
      schema,
      tracing: true,
      cacheControl: true,
      engine: false,
    });

    const app = express();

    server.applyMiddleware({ app });

    const engine = new ApolloEngine({
      apiKey: 'service:sandiiarov-7698:rv3UnQr4OVxC35oJ5pYCMw',
    });

    return engine.listen({ port, expressApp: app });
  }
}

export default Gateway;