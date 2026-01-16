/// <reference path="blocks.d.ts" />

var test = blocks.observable([1, 2, 3]);

var App = blocks.Application();

var User = App.Model({
	init: () => {

	}
});

var server = blocks.server({
	port: 8080,
	cache: false
})