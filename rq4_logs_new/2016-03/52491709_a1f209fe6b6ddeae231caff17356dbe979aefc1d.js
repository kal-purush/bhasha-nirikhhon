var path = require('path'),
	rootPath = path.normalize(__dirname + '/../../');

module.exports = {
	development: {
		rootPath: rootPath,
		port: process.env.PORT || 5000
	},
	production: {
		rootPath: rootPath,
		port: process.env.PORT || 80
	}
}