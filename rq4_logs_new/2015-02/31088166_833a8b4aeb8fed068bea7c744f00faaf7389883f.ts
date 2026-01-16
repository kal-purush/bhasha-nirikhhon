///<reference path="../bower_components/DefinitelyTyped/jquery/jquery.d.ts"/>
///<reference path="../bower_components/TypeScript.NET/build/System.d.ts" />
///<amd-dependency path="TypeScript.NET/System" />

"use strict";

import $ = require("jquery");
import ArrayUtility = System.Collections.ArrayUtility;

// Constants...
var MESSAGE:string = "Hello World";

class HelloWorld
{
	constructor(
		selector:any,
		public message:string = MESSAGE)
	{
		$(document).ready(()=>{
			$(selector).text(message);
		});
	}
}

export = HelloWorld;