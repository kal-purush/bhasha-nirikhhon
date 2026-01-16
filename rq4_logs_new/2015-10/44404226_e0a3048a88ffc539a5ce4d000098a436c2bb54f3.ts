/// <reference path="../typings/tsd.d.ts" />

var app = angular.module('myapp', ['ngRoute']);
app.config(['$routeProvider', function($routeProvider : angular.route.IRouteProvider){
	$routeProvider.when('/details/:name',{
		template: require('./views/details/details.template.html'),
		controller: 'detailsController'
	}).otherwise({
		template: require('./views/home/home.template.html'),
		controller: 'homeController'
	});
}]);