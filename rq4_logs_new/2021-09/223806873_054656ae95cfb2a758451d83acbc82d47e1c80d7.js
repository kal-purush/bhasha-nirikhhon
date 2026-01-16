"use strict";

window.GDO.Moment = {

	/**
	 * Convert PHP dateformat to moment.js
	 */
	convertFormat: function(format) {
		console.log(format);
		let replace = {
			'd': 'DD',
			'D': 'ddd',
			'j': 'D',
			'l': 'dddd',
			'N': 'E',
			'S': 'o',
			'w': 'e',
			'z': 'DDD',
			'W': 'W',
			'F': 'MMMM',
			'm': 'MM',
			'M': 'MMM',
			'n': 'M',
			't': '', // no equivalent
			'L': '', // no equivalent
			'o': 'YYYY',
			'Y': 'YYYY',
			'y': 'YY',
			'a': 'a',
			'A': 'A',
			'B': '', // no equivalent
			'g': 'h',
			'G': 'H',
			'h': 'hh',
			'H': 'HH',
			'i': 'mm',
			's': 'ss',
			'u': 'SSS',
			'e': 'zz', // deprecated since version 1.6.0 of moment.js
			'I': '', // no equivalent
			'O': '', // no equivalent
			'P': '', // no equivalent
			'T': '', // no equivalent
			'Z': '', // no equivalent
			'c': '', // no equivalent
			'r': '', // no equivalent
			'U': 'X',
		};
		return strtr(format, replace);
	},
};