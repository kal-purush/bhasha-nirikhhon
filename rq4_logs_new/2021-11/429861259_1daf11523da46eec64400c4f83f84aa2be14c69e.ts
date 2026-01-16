import 'express-async-errors';
import express from 'express';
import helmet from 'helmet';
import compression from 'compression';
import cors from 'cors';
import pinoHttp from 'pino-http';
import createHttpError from 'http-errors';

import { logger } from './config/logger';

const loggerMiddleware = pinoHttp({ logger });

const app = express();

app.use(cors());

app.use(helmet());
app.use(loggerMiddleware);
app.use(compression());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use((_req, _res, next) => {
	next(createHttpError(404, { message: 'Resource not found' }));
});

// app.use(ErrorHandler);

export default app;