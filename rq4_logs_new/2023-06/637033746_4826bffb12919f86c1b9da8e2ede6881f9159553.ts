import express, {
  NextFunction, json, Request, Response,
} from 'express';
import mongoose from 'mongoose';
import routes from './routes';

const { PORT = 3000 } = process.env;

const app = express();
app.use(json());
app.use(express.urlencoded({ extended: true }));

export interface CustomRequest extends Request {
  user?: {
    _id: string;
  };
}

app.use((req: CustomRequest, res: Response, next: NextFunction) => {
  req.user = {
    _id: '647c8164613e2ccf0314bb60',
  };
  next();
});

const logRequest = (req: Request, res: Response, next: NextFunction) => {
  console.log('Received request:', req.method, req.url);
  console.log('Request body:', req.body);
  console.log('Request query:', req.query);
  next();
};

app.use(logRequest as any);
app.use(routes);

mongoose
  .connect('mongodb://localhost:27017/mestodb')
  .then(() => {
    console.log('Connected to MondoDB');
  })
  .catch((error) => {
    console.error('Error connecting to MongoDB:', error);
  });

mongoose.set('strictQuery', false);

app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
});