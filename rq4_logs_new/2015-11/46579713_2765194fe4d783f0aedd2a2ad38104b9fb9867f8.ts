"use strict";
import "source-map-support/register";
import * as yahoo from "../yahoo/client";
import * as lib from "../lib";

yahoo.syncFinancials()
	.then(() => lib.log("done"))
	.catch(err => lib.log(err.stack));

//lib.log("!!!");