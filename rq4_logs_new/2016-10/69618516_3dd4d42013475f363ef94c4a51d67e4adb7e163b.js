/*
 * description : Parse and display the JSON config object of the current node.
 * Author : GP
 * Licence : none
*/

const json = require('format-json');
const config = require('../config.json');
module.exports = function(req, res, next) {
	
    res.write(json.plain(config));
	    next();
};