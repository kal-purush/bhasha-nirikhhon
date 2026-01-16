import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';

import routes from './routes';

const app = express();

dotenv.config();

app.listen(process.env.PORT, () => console.log(`Server listening at http://localhost:${process.env.PORT}`));

app.use(bodyParser.json());

app.use('/', cors({
  allowedHeaders: ['Content-Type', 'Authorization']
}),routes);