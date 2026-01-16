
/// <reference path="../typings/node/node.d.ts" />
/// <reference path="../typings/validator/validator.d.ts" />

import validator = require("validator");

/**
 * Contact
 */
 //It seems that if you have imported some external module, you must export it as well
 // person.ts does not import anything and so does not have to export itself. Weird!

export class Email {
	rarh: string;
	
	constructor(public description : string, public value : string) {
		if (!validator.isEmail(value)) {
			throw new TypeError("Expected a valid email!");
		}
	}
}