/// <reference path="typings/node/node.d.ts" />
/// <reference path='typings/quinn/quinn.d.ts' />
import {createServer} from 'http';

import {default as quinn} from 'quinn';
import {json, default as respond} from 'quinn/respond';
import {GET, createRouter} from 'wegweiser';

class MyResource {
	@GET("/mine")
	getMine() {
		return respond().body('Mine!\n');
	}

	@GET("/json")
	getJson() {
		return json({ error: 'Something bad(ish)!' }).status(400);
	}
}

const router = createRouter(MyResource);
const server = createServer(quinn(router));
server.listen(8000, () => {
	console.log('Listening.');
});