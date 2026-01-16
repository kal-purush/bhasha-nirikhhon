import cors from 'cors';
import express, { Application } from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import compression from 'compression';
import indexRoute from './routes';
import { defaultErrorHandler } from './middlewares';
import { config } from './config';

export const middlewares = (app: Application): express.Application => {
  app.enable('trust proxy');
  app.use(compression());
  app.use(cors());
  app.use(helmet());
  app.use(express.json({ limit: '100mb' }));
  app.use(
    express.urlencoded({
      extended: true,
      limit: '100mb',
    }),
  );

  if (config.APP.ENV !== 'test') {
    app.use(morgan('dev'));
  }

  app.use(indexRoute);
  defaultErrorHandler(app);

  return app;
};