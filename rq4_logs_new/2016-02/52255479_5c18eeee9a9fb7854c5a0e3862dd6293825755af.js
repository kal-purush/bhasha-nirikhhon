
"use strict";

var express = require('express');
var config = require('./config');

var app = express();

app.get('/', function (req, res) {
	var timeout = config['defaultTimeout'];
	var queryTimeout = parseInt(req.query['timeout']);
	if (!Number.isNaN(queryTimeout)) {
		timeout = queryTimeout; }
	setTimeout(() => res.json({
		timeout }), timeout);
});

var host = process.env['HOST'] || config['host'];
var port = process.env['PORT'] || config['port'];
var server = app.listen(port, host, () =>
	console.log(`ease started @ ${host}:${port}`));