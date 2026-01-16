var nconf = require("nconf");

function config() {

	nconf.argv().env("_");
	var environment = nconf.get("NODE:ENV") || nconf.file( "default", "./config.json");
}

config.prototype.get = function( key ) {
	return nconf.get(key);
}

module.exports = new config();