"use strict";

import {VisualStudio} from "./main";

VisualStudio.loadConfiguration()
	.then(
		configuration => {
			console.dir(configuration);
		}
	)
	.catch(
		error => {
			console.log("Caught by loadConfiguration callback -", error);
		}
	);