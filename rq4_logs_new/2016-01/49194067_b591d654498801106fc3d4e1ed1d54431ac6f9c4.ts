/// <reference path="../typings/express/express.d.ts" />

import fs = require('fs');
import Express = require('express');

export function configure(expressApp: Express.Express) {
	expressApp.get(/.*/i, (req: Express.Request, res: Express.Response) => {
		res.type('text/html');
		if (req.url !== '/') {
			res.statusCode = 404;
		}
		fs.readFile('./html/default.html', (err: NodeJS.ErrnoException, data: Buffer) => {
			res.write(data);
			res.end();
		});
	});
};