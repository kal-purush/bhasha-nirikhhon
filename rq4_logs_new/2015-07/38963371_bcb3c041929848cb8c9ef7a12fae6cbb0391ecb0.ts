// from public
// tsc --watch --sourceMap --outDir dist --module system -t es5 app/**/*.ts
// jspm bundle-sfx dist/app dist/bundle.js -m
 
/// <reference path="../typings/tsd.d.ts" />

// Globals
import globalAngular from 'angular';
import globalUiRouter from 'angular-ui-router';

// Component routes
import HomeRouterModule from './routes/home/home.route';
import AboutRouterModule from './routes/about/about.route'; 

// Common
import navbarComponentModule from './common/navbar/navbar';

// pull in gloabl libs
globalUiRouter; 
globalAngular;

// app wrapper
angular.module('app', [
	'ui.router',
	navbarComponentModule.name,
	HomeRouterModule.name,
	AboutRouterModule.name
])
.directive('app', function appComponent () {
	return {
		templateUrl:  'app/app.html',
		restrict: 'E'
	};
});

//boot
angular.bootstrap(document, ['app']);