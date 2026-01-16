import express, { Request, Response, NextFunction } from 'express';
import 'express-async-errors';
import { json } from 'body-parser';
import cookieSession from 'cookie-session';
import { errorHandler, NotFoundError, currentUser } from '@gravitaz/common';
import { createTicketRouter } from './routes/createticket';
import { showTicketRouter } from './routes/showticket';
import { showAllTicketsRouter } from './routes/showall';
import { updateTicketRouter } from './routes/updateticket';

const app = express();
app.set('trust proxy', true);
app.use(json());
app.use(
  cookieSession({
    signed: false,
    secure: process.env.NODE_ENV !== 'test',
  })
);
app.use(currentUser);

app.use(updateTicketRouter);
app.use(createTicketRouter);
app.use(showTicketRouter);
app.use(showAllTicketsRouter);

app.use('*', async (req: Request, res: Response) => {
  throw new NotFoundError(req);
});
app.use(errorHandler);

export { app };