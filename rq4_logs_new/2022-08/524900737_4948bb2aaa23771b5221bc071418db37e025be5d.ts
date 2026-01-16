import express, { Express, Request, Response } from 'express';
import userRoutes from './routes/userRoutes'
import authRoutes from './routes/authRoutes'
import courseRouter from './routes/courseRoutes'
import enrollmentRoutes from './routes/enrollmentRoutes'

const app: Express = express();

app.get('/', (req: Request, res: Response) => {
    res.send('Express + TypeScript: CMS');
});

app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.use('/', userRoutes);
app.use('/', authRoutes);
app.use('/', courseRouter);
app.use('/', enrollmentRoutes);

export default app;