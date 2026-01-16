import express from 'express';
import { urlencoded, json } from 'body-parser';
import { mongoDBConnectionString, mongoOptions, port } from './config';
import { routes } from './routes';
import { connect } from './connectToMongo';
import dotenv from 'dotenv';

const result = dotenv.config();

connect(mongoDBConnectionString, mongoOptions);

const app = express();

app.use(urlencoded({ extended: true }));
app.use(json());

routes(app);

app.listen(port, () => {
  console.log(`Server is running at: ${port}`);
})