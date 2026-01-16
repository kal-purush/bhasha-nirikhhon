import * as dotenv from 'dotenv';

dotenv.config();
let path;

console.log(process.env.NODE_ENV);
switch (process.env.NODE_ENV) {
  case 'test':
    path = `${__dirname}/../../.env.test`;
    break;
  case 'production':
    path = `${__dirname}/../../.env.production`;
    break;
  default:
    path = `${__dirname}/../../.env`;
}
dotenv.config({ path: path });

export const APP_ID = process.env.REACT_APP_API_URL;