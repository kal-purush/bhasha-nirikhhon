///<reference path="typings/angularjs/angular.d.ts"/>
///<reference path="typings/angularjs/angular-route.d.ts"/>

"use strict";

var app : ng.IModule = angular.module("picrossApp", ['ngRoute', 'controllers', 'filters', 'services']);

app.config(['$routeProvider',
    function ($routeProvider : ng.route.IRouteProvider)
    {
        $routeProvider.
            when('/:schemeId', {
                templateUrl: 'partials/show_picross.html',
                controller: 'picrossCtrl'
            }).
            when('/error/:status', {
                templateUrl: 'partials/error_page.html',
                controller: 'errorCtrl'
            });

        $routeProvider.otherwise("/1");
    }]);