import { ApolloClient, InMemoryCache, ApolloLink, HttpLink } from '@apollo/client';


export default () => {
  const cache = new InMemoryCache();

  const client = new ApolloClient({
    uri: 'https://app.asianaauto.ru/graphql',
    headers: {
      Authorization: `Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MTE4MjYxMDUsImV4cCI6MTY0MzM2MjEwNSwidXNlcl9pZCI6MSwiZGV2aWNlX2lkIjoxfQ.nx996TVwtyi5PCPBxLKXXyyc1eDcXc4ud8SqcZFWgI0`,
      'Cache-Control': 'no-cache',
      'Content-Type': 'application/json',
    },
    cache,
  });

  return client;
};