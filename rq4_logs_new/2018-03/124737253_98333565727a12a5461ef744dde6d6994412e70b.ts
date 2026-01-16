import { createConnection } from 'typeorm';
import * as path from 'path'

import User from './entities/User';

// Closure pattern to invoke it instantly and store the promise for later use
const init = (async () => createConnection({
  type: "postgres",
  host: "postgres",
  port: 5432,
  username: "root",
  password: "admin",
  database: "test",
  entities: [
    path.join(__dirname, "/entities/*.js")
  ],
  synchronize: true
}))();

/**
 * Saves a new user into the database for later use. Creates a UserEntity and returns a User.
 * @param email Email address for a new user.
 */
const addUser = async (email: string) => init.then(async connection => {
  try {
    const newUser = new User();
    newUser.email = email;
    await newUser.save();
    return newUser.toJson();
  } catch (error) {
    console.log(`Could not save UserEntity for new user: ${email}.`);
    console.log(`Error from saving: ${JSON.stringify(error, null, 2)}.`);
    throw error;
  }
});

export default {
  addUser
}