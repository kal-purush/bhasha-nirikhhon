/// <reference path="typings/tsd.d.ts" />

import $ = require("jquery");

module SampleModule{
	export class Greeter<T> {
        
        static GLOBAL: string = "GLOBAL2";
    	greeting: T;
    	constructor(message: T) {
        	this.greeting = message;
    	}
	    greet() {
	    	//var data: string = <string><any>this.greeting;
	    	$(".log").text(<string><any>this.greeting);
	        return this.greeting;
	    }
	}
}

var sample = new SampleModule.Greeter<string>("text");
sample.greet();