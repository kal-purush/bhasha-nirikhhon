'use strict';

var winston = require('winston');
module.exports = exports = winston;

exports.loggly = function loggly(options) {
	options = options || {};
	require('winston-loggly');
	var logger = options.logger || winston;

	logger.add(winston.transports.Loggly, {
		level: options.level || process.env.LOGGLY_LEVEL || 'warn',
		subdomain: process.env.LOGGLY_DOMAIN,
		inputToken: process.env.LOGGLY_TOKEN,
		tags: options.tags,
		json: options.json
	});
};

exports.removeConsole = function removeConsole(logger) {
	logger = logger || winston;
	return logger.remove(logger.transports.Console);
};