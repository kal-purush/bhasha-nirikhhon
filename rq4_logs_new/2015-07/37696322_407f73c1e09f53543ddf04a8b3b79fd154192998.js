'use strict';

var express = require('express');

module.exports = function(resources) {
	
	var app = express();

	app.use(function (req, res, next) {
		if (!req.query.token) {
			res.status(400).send('Unauthorized access.');
			return next(new Error('Unauthorized access.'));
		}
		next();
	});
	app.use('/api', require('./api/phone-numbers')(resources));

	return app;
}