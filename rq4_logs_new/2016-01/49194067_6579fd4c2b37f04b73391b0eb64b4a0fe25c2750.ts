/// <reference path="./typings/express/express.d.ts" />
/// <reference path="./typings/express-ws/express-ws.d.ts" />
/// <reference path="./typings/body-parser/body-parser.d.ts" />
import Express = require('express');
import ExpressWs = require('express-ws');
import BodyParser = require('body-parser');

const app: Express.Express = Express();
const appWs = ExpressWs(app);

app.use(BodyParser.urlencoded({
	extended: true
}));

import route_webhook = require('./routes/webhook');
import route_wsNotify = require('./routes/wsNotify');

route_webhook.configure(appWs);
route_wsNotify.configure(appWs);

app.listen(process.env.PORT || 80);