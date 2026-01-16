import checkInitializeProjectSettings from '@lib/startup/checkInitialProjectSettings';
import io from '@src/io';
import { connectDBTest, wrapConnectDbWithSync } from '@lib/startup/connectDB';
import { ServerStarter } from 'express-quick-builder';
import cron from '@lib/middlewares/cron';
import morgan from '@lib/middlewares/morgan';
import cors from 'cors';
import helmet from 'helmet';
import { RateLimiter } from '@util/Middleware';
import Assets from '@util/Assets';
import ipfilter from '@lib/middlewares/ipfilter';
import fileUploader from 'express-fileupload';
import express from 'express';
import packageJson from '../package.json';

checkInitializeProjectSettings();

const PORT: number = parseInt(
  process.env.NODE_ENV === 'production'
    ? process.env.PROD_PORT || '4000'
    : process.env.DEV_PORT || '4000',
  10,
);

const REQUEST_HANDLERS = [
  morgan(),
  cors({
    origin:
      process.env.NODE_ENV === 'development'
        ? '*'
        : process.env.REQUEST_URI || '*',
  }),
  helmet(),
  RateLimiter(),
  // Assets.wrapper(ipfilter),
  fileUploader({
    limits: { fileSize: 50 * 1024 * 1024 },
    useTempFiles: true,
    tempFileDir: '/tmp/file/',
    debug: process.env.NODE_ENV === 'development',
  }),
  express.static('public'),
  express.json({ limit: '100mb' }),
  express.urlencoded({ extended: true, limit: '100mb' }),
];

const STARTUP_EXECUTES = [cron];

const SERVER_STARTER_PROPERTIES = {
  routePath:
    process.env.NODE_ENV !== 'production'
      ? `${process.cwd()}/src/routes`
      : `${process.cwd()}/dist/routes`,
  requestHandlers: REQUEST_HANDLERS,
  executes: STARTUP_EXECUTES,
  portStrict:
    process.env.NODE_ENV === 'production'
      ? true
      : process.env.PORT_STRICT === 'true'
      ? true
      : false,
  appName: packageJson.name,
};

export function Root(port = PORT): ReturnType<typeof ServerStarter> {
  wrapConnectDbWithSync();

  const server = ServerStarter({ ...SERVER_STARTER_PROPERTIES, port });

  io(server.server);
  return server;
}

export async function testRoot(
  port = PORT,
): Promise<ReturnType<typeof ServerStarter>> {
  try {
    await connectDBTest();
    const server = ServerStarter({ ...SERVER_STARTER_PROPERTIES, port });
    io(server.server);
    return server;
  } catch (e) {
    throw new Error(e);
  }
}