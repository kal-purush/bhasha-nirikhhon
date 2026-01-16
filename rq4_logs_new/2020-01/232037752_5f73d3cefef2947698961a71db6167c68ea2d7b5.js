const { prisma } = require('./prisma/generated/prisma');
const { GraphQLServer } = require('graphql-yoga');

const resolvers = {
    Query: {
      users: (root, args, context, info) => {
        return prisma.userses()
      },
    },
  }

const server = new GraphQLServer({
    typeDefs: './schema.graphql',
    resolvers,
    context: { prisma },
  })

server.start(() => console.log(`Server is running on http://localhost:4000`))