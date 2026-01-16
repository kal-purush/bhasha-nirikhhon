import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
// Temp import { notesRouter } from './router/notes.router.js';
import createDebug from 'debug';

import { errorMiddleware } from './middleware/middleware';
import { monumentsRouter } from './router/monument.routes.js';

const debug = createDebug('W7E:app');

export const app = express();
debug('Starting');

app.use(cors());
app.use(morgan('dev'));

app.use(express.json());
app.use(express.static('public'));

app.use('/tasks', monumentsRouter);
// Temp app.use('/notes', notesRouter);

app.use(errorMiddleware);