import express, { Response, Request, NextFunction } from 'express';
import mongoose, { Error } from 'mongoose';
import userRouter from './routes/users';
import cardRouter from './routes/cards';

interface ResponseError extends Error {
  statusCode?: number;
}

const { PORT = 3000 } = process.env;
const app = express();
mongoose.connect('mongodb://127.0.0.1:27017/mestodb');

app.use(express.json());

app.use('/users', userRouter);

app.use((req: any, res: Response, next: NextFunction) => {
  req.user = {
    _id: '656c73a50e6fbacdaa2a2eaa',
  };

  next();
});

app.use('/cards', cardRouter);

app.use((err: ResponseError, req: Request, res: Response, next: NextFunction) => {
  const { statusCode = 500, message } = err;

  res
    .status(statusCode)
    .send({
      message: statusCode === 500
        ? 'На сервере произошла ошибка'
        : message,
    });
  next();
});

app.listen(PORT, () => {
  console.log(`Открылся порт ${PORT} для связи с космосом`);
});