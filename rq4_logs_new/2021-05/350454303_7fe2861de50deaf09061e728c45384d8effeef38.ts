import bodyParser from 'body-parser';
import express from 'express';
import mongoose from 'mongoose';

import feedbackRoutes from './routes/feedback';

const app = express();

app.use(bodyParser.json());

const baseUrl = process.env.BASE_MONGODB || 'mongodb://root:example@localhost:27017/my-db?authSource=admin';

mongoose
  .connect(baseUrl, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => {
    console.log('Connection to mongodb base established');
  })
  .catch((ex) => {
    console.log(ex);
  });

const url = '/api/v1';

app.use(`${url}/requests`, feedbackRoutes);

export default app;