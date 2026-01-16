import { Schemas } from './gateway';

export const resolvers = (schema: Schemas) => ({
  User: {
    posts: {
      fragment: `fragment UserFragment on User { id }`,
      resolve(parent: { id: string }, args: any, context: any, info: any) {
        return info.mergeInfo.delegateToSchema({
          schema: schema['post'],
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
});