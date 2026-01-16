import { HttpLink } from 'apollo-link-http';
import {
  introspectSchema,
  makeRemoteExecutableSchema,
  mergeSchemas,
} from 'graphql-tools';
import fetch from 'isomorphic-fetch';
import { resolvers } from './resolvers';
import LinkSchema from './schema';

export default async () => {
  const UserLink = new HttpLink({
    uri: 'http://localhost:4001/graphql',
    fetch,
  });
  const PostLink = new HttpLink({
    uri: 'http://localhost:4002/graphql',
    fetch,
  });

  const UserSchema = makeRemoteExecutableSchema({
    schema: await introspectSchema(UserLink),
    link: UserLink,
  });

  const PostSchema = makeRemoteExecutableSchema({
    schema: await introspectSchema(PostLink),
    link: PostLink,
  });

  return mergeSchemas({
    schemas: [UserSchema, PostSchema, LinkSchema],
    resolvers: resolvers({ user: UserSchema, post: PostSchema }),
  });
};