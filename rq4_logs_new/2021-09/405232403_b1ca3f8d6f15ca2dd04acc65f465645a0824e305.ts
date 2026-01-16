require('dotenv').config();

import { AddressInfo } from 'net';
import http from 'http';
import mongoose from 'mongoose';
import server from './server';
// constants
const port = process.env.PORT || '5000';

async function bootstrap() {
  //Add external services init as async operations

  //MongoDB Atlas database connection
  const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost/';

  const MONGO_CONFIG = {
    useCreateIndex: true,
    useNewUrlParser: true,
    useUnifiedTopology: true,
  };
  // check for any errors when connecting
  try {
    mongoose.Promise = global.Promise;
    await mongoose.connect(MONGO_URL, MONGO_CONFIG);
    console.log('Successfully connected to MongoDB Atlas!');
  } catch (err) {
    console.log('Mongoose error', err);
  }

  return http.createServer(server.callback()).listen(port);
}

bootstrap()
  // successfully runs the server and either async setups
  .then((server: http.Server) => {
    const { port } = server.address() as AddressInfo;
    console.log(`Server listening on port ${port}!`);
  })
  // log the error and exit if it fails
  .catch((err) => {
    setImmediate(() => {
      console.error('Unable to run the server because of the following error:');
      console.error(err);
      process.exit();
    });
  });