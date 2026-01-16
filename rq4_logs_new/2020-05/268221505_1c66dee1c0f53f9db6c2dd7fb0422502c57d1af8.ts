import 'reflect-metadata';
import * as express from 'express';

import {InversifyExpressServer} from 'inversify-express-utils';
import * as bodyParser from 'body-parser';
import {buildProviderModule} from 'inversify-binding-decorators';
import {Container} from 'inversify';
import {connectToDatabase} from './config/database';
import {configCors} from './config/cors.config';

import './controllers';

import models from './models/models';
import {PORT} from './environments';
import {AuthMiddleware} from './middlewares/auth.middleware';
import {TYPES} from './types/type';
import logger from './config/log';


// start the server
const container = new Container();
container.bind<AuthMiddleware>(TYPES.AuthMiddleware).to(AuthMiddleware);

models.forEach(i => container.bind<any>(i.type).toConstantValue(i.model));
container.load(buildProviderModule());
const server = new InversifyExpressServer(container);

server.setConfig((app) => {
    app.use(bodyParser.urlencoded({
        extended: true
    }));
    configCors(app);
    connectToDatabase(app);
    app.use(bodyParser.json());
    app.use('/', express.static('public'));

});

const app = server.build();
app.listen(PORT);
logger.info(`Server started on port ${PORT} :)`);

exports = module.exports = app;