(function() {
	'use strict';

	var one = require('./module_one.js'),
		two = require('./module_two.js');

	one.log();
	two.log();
})();