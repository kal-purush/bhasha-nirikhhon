/// <reference path="libs/angular/angular.d.ts" />
/// <reference path="libs/angular/angular-route.d.ts" />

'use strict';

// Create and register modules
// var modules = ['app.controllers','app.directives', 'app.filters', 'app.services'];
// modules.forEach((module) => angular.module(module, []));

// *** Push ngRoute or $routeProvider won't work ***
// modules.push("ngRoute");

// angular.module('app', modules);

// Url routing
// angular.module('golby').config(['$routeProvider',
//     function routes($routeProvider: ng.route.IRouteProvider) { // *** $routeProvider is typed with ng.route.IRouteProvider ***
//         $routeProvider
//             .when('/', {
//                 templateUrl: 'views/MyView.html',
//                 controller: 'golby.controllers.BlogPostCtrl'
//             })
//             .when('/cheese', {
//                 templateUrl: 'views/Cheese.html',
//                 controller: 'golby.controllers.BlogPostCtrl'
//             })
//             .otherwise({
//                 redirectTo: '/'
//             });
//     }
// ]);