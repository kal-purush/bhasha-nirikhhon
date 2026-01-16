const {
  GraphQLObjectType,
  GraphQLString,
  GraphQLInt,
  GraphQLSchema,
  GraphQlList,
  GraphQLNonNull,
} = require('graphql');


//hardcoded data
const customers = [
  {id: '1', name: 'Rahmat', email: 'rezaei@adawarp.com', age: 27 },
  {id: '2', name: 'Adaniya', email: 'adaniya@adawarp.com', age: 25 },
  {id: '3', name: 'Zigen', email: 'zigen@adawarp.com', age: 25 },
]

// Customer type
const CustomerType = new GraphQLObjectType({
  name: 'Customer',
  fields: () => ({
    id: {type: GraphQLString},
    name: {type: GraphQLString},
    email: {type: GraphQLString},
    age: {type: GraphQLInt},
  })
})


//    Root Query
const RootQuery = new GraphQLObjectType({
  name: 'RootQueryType',
  fields: {
    customer: {
      type: CustomerType,
      args: {
        id: {type: GraphQLString}
      },
      resolve(parentValue, args) {
        for (let i = 0; i < customers.length; i++) {
          if (customers[i].id == args.id) {
            return customers[i];
          }
        }
      }
    }
  }
})

module.exports = new GraphQLSchema({
  query: RootQuery
})