import express from 'express';
import compression from 'compression';
import bodyParser from 'body-parser';
import cookieParser from 'cookie-parser';
import 'express-async-errors'; // This library is about what happens when you hit an error
import routerMiddleware from './routers';
import errorhandlerMiddleware from './middlewares/errorhandler';
const app = express();

// setup healthCheck
app.use('/monitor/ping', (_, res) => {
  res.json('ok');
});

app.set('trust proxy', ['loopback']);
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));
app.use(cookieParser());
app.use(compression()); // gzip
app.use('/api', routerMiddleware); // Main Routers
app.use('*', (_, res) => res.status(404)); // 404 Handler
app.use(errorhandlerMiddleware); // error handler - fix no enign error
export default app;