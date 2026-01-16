const { ApolloServer, gql, PubSub } = require("apollo-server");

const typeDefs = gql`

    type Query {
      greet: String!
    }

    input User {
      username: String!
      password: String!
    }

    type Mutation {
      login(user: User!): String!
    }

    type Subscription {
      loginNotification: String!

    }
`;

const LOGIN_NOTIFICATION = 'LOGIN_NOTIFICATION';

const resolvers = {
  Query: {
    greet: () => "Hello World from Apollo GraphQL"
  },

  Mutation: {
    login: (parent, args, context) => {

      console.log("******", args);

      const {user: {username}} = args;

      const {pubsub} = context;

      pubsub.publish(LOGIN_NOTIFICATION, {
        loginNotification: ` Successfully ${username} login`
      });
      return ` Successfully ${username} login`;
    }
  },
  Subscription: {
    loginNotification: {
      subscribe: (_, __, { pubsub }) => pubsub.asyncIterator(LOGIN_NOTIFICATION)
    }
  },
}

const pubsub = new PubSub();

const server = new ApolloServer({
  typeDefs,
  resolvers,
  context: ({ req, res }) => ({ req, res, pubsub })
});

server.listen().then(({ url }) => console.log(`server started at ${url}`));



/*

This works only apollo server 2 




1. Subscription Type loginNotification resolver waiting for "LOGIN_NOTIFICATION" to publish by resolver

2. Mutation Type login  Resolver publish an event called "LOGIN_NOTIFICATION" with loginNotification Subscription resolver

subscription{
  loginNotification
}

mutation {
  login(user: {username: "Raj", password: "123"})
}

query {
  greet
}


*/