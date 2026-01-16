import { ApolloEngine } from 'apollo-engine';
import { HttpLink } from 'apollo-link-http';
import { ApolloServer } from 'apollo-server-express';
import express from 'express';
import fs from 'fs';
import { GraphQLSchema } from 'graphql';
import { makeRemoteExecutableSchema, mergeSchemas } from 'graphql-tools';
import fetch from 'isomorphic-fetch';
import { resolvers } from './resolvers';

interface Config {
  [name: string]: {
    definition: string;
    endpoint: string;
  };
}

const APOLLO_ENGINE_KEY = 'service:sandiiarov-7698:rv3UnQr4OVxC35oJ5pYCMw';

class Gateway {
  private config: Config;

  public constructor(config: Config) {
    this.config = config;
  }

  private makeSchema(path: string, uri: string) {
    return makeRemoteExecutableSchema({
      schema: fs.readFileSync(path, 'utf8'),
      link: new HttpLink({ uri, fetch }),
    });
  }

  private makeSchemas() {
    const schemas: { [name: string]: GraphQLSchema } = Object.entries(
      this.config
    ).reduce(
      (config, [name, { definition, endpoint }]) => ({
        ...config,
        [name]: this.makeSchema(definition, endpoint),
      }),
      {}
    );

    return mergeSchemas({
      schemas: [
        ...Object.values(schemas),
        fs.readFileSync('./src/schema/link.graphql', 'utf8'),
      ],
      resolvers: resolvers(schemas),
    });
  }

  public listen({ port }: { port: string | number }) {
    const app = express();
    const engine = new ApolloEngine({ apiKey: APOLLO_ENGINE_KEY });

    const server = new ApolloServer({
      schema: this.makeSchemas(),
      introspection: true,
      engine: false,
    });

    server.applyMiddleware({ app });

    engine.listen({ port, expressApp: app }, () => {
      console.log(`🚀 RUNNING @ ${port}`);
    });
  }
}

export default Gateway;