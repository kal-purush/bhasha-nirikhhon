import { GraphQLObjectType, GraphQLSchema, GraphQLString } from 'graphql';

export const graphQLSchema = new GraphQLSchema({
    query: new GraphQLObjectType({
        name: 'hello',
        fields: () => ({
            hello: {
                type: GraphQLString,
                resolve: () => 'Hello world'
            }
        })
    })
});