import * as glob from 'glob'
import * as path from 'path'
import { mergeTypes } from 'merge-graphql-schemas'
import { importSchema } from 'graphql-import'



const modulesPath = path.join(__dirname, '/modules')

export const getResolvers = () => {
  const resolvers = glob
    .sync(`${modulesPath}/**/resolvers.?s`)
    .map(resolver => require(resolver).resolvers);

    return resolvers
}

export const getTypeDefs = () => {
  const graphqlTypes = glob
    .sync(`${modulesPath}/**/*.graphql`)
    .map(x => importSchema(x))

  return mergeTypes(graphqlTypes, { all: true })
}