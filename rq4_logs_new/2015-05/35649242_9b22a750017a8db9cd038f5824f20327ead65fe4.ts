
// This is an "internal" module native to Typescript.  This file will have a 
// module defined in it and then made availible on the page.  Typescript will
// put the module on the global namespace. (Thus the reason for using the 'modules').

/// <reference path="./message" />

// To use within something like Webpack, that wraps all files in a closure, we ned to 
// treat internals like externals. Adding a line like below in addition to the 
// referance path above. 
//
// (CON: typescript doesn't like this, doesn't know we are using aditional tooling)
import messages = require('exports?Messages!./message');

// This is an "external" modules.  
var lodash = require('lodash');

module TypescriptApp {
	export class App {
		helper:Function;
		
		constructor(helper: Function) {
			this.helper = helper;
		}
		
		run():void {
			this.helper();
		}
	}
}


document.addEventListener("DOMContentLoaded", (event) => { 
	let myTypescript = new TypescriptApp.App(messages.message);
	myTypescript.run();
});