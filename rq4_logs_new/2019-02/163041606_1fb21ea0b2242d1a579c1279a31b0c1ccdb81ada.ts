import { ApolloServer } from 'apollo-server';
import fs from 'fs';
import { printSchema } from 'graphql';
import { mergeSchemas } from 'graphql-tools';
import { resolvers } from '../resolvers';
import { splitSchemas } from './splitSchemas';

export interface Config {
  [name: string]: {
    definition: string;
    endpoint?: string;
  };
}

class Gateway {
  private config: Config;

  public constructor(config: Config) {
    this.config = config;
  }

  private makeSchema() {
    const { remote, local } = splitSchemas(this.config);

    return mergeSchemas({
      schemas: [...Object.values(remote), ...Object.values(local)],
      resolvers: resolvers(remote),
    });
  }

  public generateSchema() {
    fs.writeFileSync('./schema/schema.graphql', printSchema(this.makeSchema()));
  }

  public listen(port: string | number) {
    const server = new ApolloServer({
      schema: this.makeSchema(),
      introspection: true,
    });

    return server.listen({ port });
  }
}

export default Gateway;