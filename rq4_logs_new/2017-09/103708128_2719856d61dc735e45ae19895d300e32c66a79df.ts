import { ApolloClient, createNetworkInterface } from 'apollo-client';

const networkInterface = createNetworkInterface({ uri: 'https://api.graph.cool/simple/v1/cj7wkwti70igu0138ut305g7w' })

const client = new ApolloClient({ networkInterface });

export function provideClient(): ApolloClient {
  return client;
}