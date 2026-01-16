/// <reference path="../../../typings/tsd.d.ts" />

namespace app {
    'use strict';

    var app = angular.module('app', [
        'ngResource',
        'ngRoute',
        'ngAnimate'
    ]);

    app.config(routeConfig);

    routeConfig.$inject = ['$routeProvider'];
    function routeConfig($routeProvider: ng.route.IRouteProvider): void {
        $routeProvider
            .when('/', {
                templateUrl: '/app/views/opEnv.html',
                controller: 'OpEnvController as vm'
            })
            .when('/addPerson', {
                templateUrl: '/app/views/person.html',
                controller: 'PersonController as vm'
            })
            .when('/addGroup', {
                templateUrl: '/app/views/group.html',
                controller: 'GroupController as vm'
            })
            .otherwise('/');
    }
}