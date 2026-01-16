const express = require('express');
const typeDefs = require('./src/schema');
const resolvers = require('./src/resolvers');
const models = require('./src/models');
const jwt = require('jsonwebtoken');
const { ApolloServer } = require('apollo-server-express');
const db = require('./src/db');

require('dotenv').config()

// Connect to the database
const MONGO_HOSTNAME = process.env.MONGO_HOSTNAME;
const MONGO_PORT = process.env.MONGO_PORT;
const MONGO_DB = process.env.MONGO_DB;
const DB_URL = process.env.MONGO_URL;
db.connect(DB_URL);

const port = process.env.PORT


// get the user info from a JWT
const getUser = token => {
  if (token) {
      try {
          // return the user information from the token
          //return jwt.verify(token, process.env.JWT_SECRET);
          return jwt.verify(token, process.env.JWT_SECRET);
      } catch (err) {
          // if there's a problem with the token, throw an error
          throw new Error('Session invalid');
      }
  }
}; 

const app = express();


// create an instance of Apollo Server 
const server = new ApolloServer({ 
  typeDefs, 
  resolvers,
  context: ({ req }) => {
     // get the user token from the headers
      const token = req.headers.authorization;
      // try to retrieve a user with the token
      const user = getUser(token);
      // for now, let's log the user to the console:
      console.log(user);
      // add the db models and the user to the context
      return { models, user };
  }
});


async function startServer() {
  await server.start();
  server.applyMiddleware({ app, path: '/api' });
}

startServer().then(() => {
  app.get('/', (req, res) => res.send('Hello World'));
  
  /*
  app.listen({ port }, () =>
    console.log(`Server running at http://localhost:${port}`)
  );
  */

  app.listen(process.env.PORT || 9001, () => console.log('Server Running'))
});