import { ApolloServer, mergeSchemas } from 'apollo-server';
import { getSchemas } from './getSchemas';

const linkTypeDefs = `
  extend type User {
    posts: [Post]
  }
`;

(async () => {
  const [userSchema, postSchema] = await getSchemas([
    'http://localhost:4001/graphql',
    'http://localhost:4002/graphql',
  ]);

  const schema = mergeSchemas({
    schemas: [userSchema, postSchema, linkTypeDefs],
    resolvers: {
      User: {
        posts: {
          fragment: `fragment UserFragment on User { id }`,
          resolve(parent: { id: string }, args: any, context: any, info: any) {
            return info.mergeInfo.delegateToSchema({
              schema: postSchema,
              operation: 'query',
              fieldName: 'posts',
              args: {
                userId: parent.id,
              },
              context,
              info,
            });
          },
        },
      },
    },
  });
  const server = new ApolloServer({ schema });
  const { url } = await server.listen({ port: 4000 });

  console.log(`🚀  Server ready at ${url}`);
})();