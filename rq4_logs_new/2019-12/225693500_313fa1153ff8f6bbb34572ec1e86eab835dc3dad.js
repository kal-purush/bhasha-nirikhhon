import ApolloClient from 'apollo-boost';
//https://48p1r2roz4.sse.codesandbox.io
export const client = new ApolloClient({
  uri: 'https://colourlovers-graphql-api.herokuapp.com/graphql',
});