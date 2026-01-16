/// <reference path="../../typings/angularjs/angular.d.ts" />
/// <reference path="../../typings/angularjs/angular-route.d.ts" />
/// <reference path="controller/hello.ts" />


function routeConfig($routeProvider: ng.route.IRouteProvider, $locationProvider: ng.ILocationProvider) {
    $locationProvider.html5Mode(true);
    $routeProvider.when('/', {
        controller: App.Ctrl.Hello,
        templateUrl: "templates/hello.html"
    })
    .when('/:name', {
        controller: App.Ctrl.Hello,
        templateUrl: "templates/hello.html"
    })
    .otherwise({
        redirectTo: '/'
    });
};
routeConfig.$inject = ['$routeProvider', '$locationProvider'];

let app = angular.module("App", ['ngRoute']);
app.config(routeConfig);