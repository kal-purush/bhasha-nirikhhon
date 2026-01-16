import * as dotenv from 'dotenv';
import * as path from 'path';

if (process.env.TEST === 'OK') {
  dotenv.config({ path: path.join(__dirname, '..', '..', '/.test.env') });
} else {
  dotenv.config();
}

import 'reflect-metadata';
import { Server } from 'server';
import { Container } from 'typedi';
import { useContainer as useClassValidatorContainer } from 'class-validator';
import { useContainer as useTypeContainer } from 'typeorm';

useClassValidatorContainer(Container);
useTypeContainer(Container);

const laodApp = async () => {
  const server = new Server();
  await server.connectDb();
  await server.run();
};

export default { run: laodApp };