import cors from "cors";
import express from "express";
import { ApolloServer, gql } from "apollo-server-express";

const app = express();

// app.use(cors);

const schema = gql`
  type Query {
    me: User
  }
  type User {
    username: String!
  }
`;

const resolvers = {
  Query: {
    me: () => {
      return {
        username: "Robin Wieruch"
      };
    }
  }
};

const server = new ApolloServer({
  typeDefs: schema,
  resolvers
});

server.applyMiddleware({ app, path: "/graphql" });

const port = 5000;

app.get("/api", (req, res) => {
  res.send({ status: "ok" });
});

app.listen({ port }, () => {
  console.log(`Apollo Server on http://localhost:${port}/graphql`);
});