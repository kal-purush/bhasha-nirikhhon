/// <reference path="../../typings/tsd.d.ts" />
"use strict";
import * as fs from 'fs';
import * as zmq from 'zmq';

// Create publisher endpoint
let publisher = zmq.socket('pub');
let filename = process.argv[2];

fs.watch(filename, () => {
	// send the message to any subscribers
	publisher.send(JSON.stringify({
		type: 'changed',
		file: filename,
		timestamp: Date.now()
	}));
});

publisher.bind('tcp://*:5444', (err) => {
	if(err) { throw err; }
	console.log('Listening for ZMQ subscribers');
});