/// <reference path="./onoff.ts" />

import * as onoff from "./onoff";

var app = new onoff.OnoffApp();

var computers = [
	new onoff.Computer("Home", "23:43:34:23:23")
];
app.start("trianesturineatuirneastvxtlens", computers);