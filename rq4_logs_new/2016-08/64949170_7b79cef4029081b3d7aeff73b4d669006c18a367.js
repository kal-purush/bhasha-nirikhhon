var app = angular.module("myApp", ["ngRoute"]);
app.config(function($routeProvider) {
    $routeProvider
	 .when("/", {
        templateUrl : 'templates/main.html'
    })
    .when("/apartments", {
        templateUrl : 'templates/apartments.html'
    })
    .when("/townhouses",{
        templateUrl : 'templates/townhouses.html'
    })
    .when("/independent", {
        templateUrl : 'templates/independent.html'
    });
});