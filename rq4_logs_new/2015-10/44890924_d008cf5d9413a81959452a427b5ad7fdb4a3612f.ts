///<reference path="../../typings/tsd.d.ts" />
///<reference path="../../typings/typescriptApp.d.ts" />

module Elife {
    angular
        .module('elife')
        .config([
            '$stateProvider',
            '$urlRouterProvider',
            '$ocLazyLoadProvider',
            '$locationProvider',
            config
        ]);

    function config($stateProvider, $urlRouterProvider, $ocLazyLoadProvider, $locationProvider) {

        //$locationProvider.html5Mode(true).hashPrefix('!');

        $ocLazyLoadProvider.config({
            //debug: true
        });

        $urlRouterProvider.otherwise('/app/play');

        $stateProvider
            // Main Layout Structure
            .state('app', {
                abstract: true,
                template: '<ui-view></ui-view>',
                url: '/app'
            })
        ;

    }
}