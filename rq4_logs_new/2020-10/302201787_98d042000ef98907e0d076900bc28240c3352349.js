const express = require("express");
const { graphqlHTTP } = require("express-graphql");

const { graphql, buildSchema } = require("graphql");

const schema = buildSchema(`
  type Query {
    hello: String
  }
`);

const root = { hello: () => "Hello world" };

const server = express();

server.use(
  "/graphql",
  graphqlHTTP({
    schema,
    rootValue: root,
    graphiql: true,
  })
);

server.listen(5000);