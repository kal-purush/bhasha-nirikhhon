const express = require('express')
const expressGraphQL = require('express-graphql')
const app = express();

var schema = require('./src/typeDef').schema;

app.use('/graphql', expressGraphQL({
  schema: schema,
  graphiql: true
}))
app.listen(5000, () => console.log('Server Running'))