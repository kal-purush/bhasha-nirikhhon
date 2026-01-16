import * as dotenv from 'dotenv';

dotenv.config();
const email = process.env.TWITTER_EMAIL;
const uname = process.env.TWITTER_USERNAME;
const password = process.env.TWITTER_PASSWORD;

Feature('twitter_bookmark');

Scenario('test something',  ({ I }) => {

});