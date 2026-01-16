'use strict';

import * as express from 'express';
import * as conf from 'nconf';

conf.argv().env().file({ file: __dirname + '/../config.json' });

const IP_ADDRESS = conf.get('IP_ADDRESS');
const PORT = conf.get('PORT');

let app = express();

app.use('/', express.static('public'));

app.listen(PORT, IP_ADDRESS);
console.log('Server running at http://' + IP_ADDRESS + ':' + PORT);