import * as express from 'express';
import * as bodyParser from 'body-parser';
import * as path from 'path';
import * as morgan from 'morgan';
import router from './routes/api';
import DBConnection from './util/db';
import { index } from './http/controllers/home';

const app = express();

// connect to db
const db = new DBConnection();
db.connect();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(
    '/static',
    express.static(path.join(__dirname, '/../assets'), { maxAge: 31557600000 })
);
app.use(morgan('combined'));
app.set('view engine', 'pug');
app.set('views', path.join(__dirname, '/../resources/views'));

app.use('/api', router);
app.get('/*', index);

export default app;