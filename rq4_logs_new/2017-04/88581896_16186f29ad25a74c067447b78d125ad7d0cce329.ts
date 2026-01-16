import {ExpressServer} from '../core/classes/ExpressServer';
import config from '../settings/index';
import * as path from 'path';
import * as fs from 'fs';
import apiRoutes from './routers/api';
import webappsRoutes from './routers/webapps';

// Define Server
let server  = new ExpressServer();
// Setup Server
server.useRouter('/api', apiRoutes.router);

server.useRouter('/view', webappsRoutes.router);

// Start Server
server.listen(config.server.port).then(() => {
    console.log('Server up at port '+config.server.port);
});