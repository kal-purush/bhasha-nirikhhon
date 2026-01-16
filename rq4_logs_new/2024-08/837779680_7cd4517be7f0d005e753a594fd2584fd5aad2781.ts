import express, { Express, Request, Response, NextFunction } from 'express';
import morgan from 'morgan';
import authRouter from './routes/authRoutes';
import errorController from './controllers/errorController';
import rateLimit from 'express-rate-limit';
import { endpointNotImplemented, tooManyRequests } from './controllers/suspicionController';

const app: Express = express();

app.use(morgan('dev')); // to log any http request to the server
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(rateLimit({
    windowMs: 1000 * 60, // time in ms
    limit: 500,
    handler: tooManyRequests    
})); // limit the number of requests sent to the server to under 500 requests per minute.

app.use('/api/auth', authRouter); // authentication routes

app.route('*').all(endpointNotImplemented); // whenever a user sends a request to an unimplemented endpoint, they will get a 404 status code response

app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
    errorController(err, req, res, next)
}); // pass errors to the global error controller

export default app;