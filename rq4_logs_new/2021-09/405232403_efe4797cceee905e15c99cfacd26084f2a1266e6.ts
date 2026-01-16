import { Context } from 'koa';

import User from './user.model';
import generateId from '../../utils/generateId.util';

// TODO: Think of the functions for the user

export const register = async (ctx: Context) => {
  try {
    const { username } = ctx.request.body;

    ctx.assert(
      username,
      400,
      'Username is missing, something went wrong with the registration request.',
    );

    // check if username is already taken
    const user = await User.findOne({ username });
    ctx.assert(!user, 409, 'Username is already taken.');

    // create new user object in mongoose
    const newUser = new User({
      username,
      toysOwned: [],
      registerDate: new Date(),
    });
    const savedUser = await newUser.save();
    ctx.assert(
      savedUser,
      500,
      'Failed to store user information on the database.',
    );

    // return success status code and JSON of the user object
    ctx.status = 200;
    ctx.body = savedUser;
  } catch (err) {
    // send it to the error middleware
    throw err;
  }
};

export const retrieveUserInfo = async (ctx: Context) => {
  try {
    const { username } = ctx.params;
    ctx.assert(
      username,
      400,
      'Username is missing, something went wrong with authentication.',
    );

    // check if user exists, and retrieve user information if so
    const user = await User.findOne({ username });
    ctx.assert(user, 404, 'Username not found in the database.');

    // return success status code and JSON of the user object
    ctx.status = 200;
    ctx.body = user;
  } catch (err) {
    // send it to the error middleware
    throw err;
  }
};