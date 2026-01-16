// part 11 (9:00) - last position. 

import * as Redis from 'ioredis';
import fetch from 'node-fetch';

import { createConfirmEmailLink } from "./createConfirmEmailLink";
import { createTypeormConn } from "./createTypeormConn";
import { User } from '../entity/User';

let userId = '';

beforeAll(async () => {
  await createTypeormConn();
  const user = await User.create({
    email: "bob5@bob.com",
    password: "awoinaslidtyao"
  }).save();
  userId = user.id;
});

test("Make sure createConfirmEmailLink works", async () => {
  const url = await createConfirmEmailLink(
    process.env.TEST_HOST as string,
    userId,
    new Redis()
  );

  const response = await fetch(url);
  const text = await response.text();
  console.log(text);

});