'use strict';

angular.module('app', [
    
                'ngResource',
                'ui.router',
                // 'ngAnimate',
                'angularUtils.directives.dirPagination',
                'angularModalService'
            ])


.value('Config', {
    youtubeKey : 'AIzaSyCqR1vxxGI7Qz9W-N3IJDF0E1-9DoPuhjw'     
})


.config(['$stateProvider', '$urlRouterProvider', '$locationProvider', function($stateProvider, $urlRouterProvider, $locationProvider) {

    $urlRouterProvider.when('/dashboard', '/dashboard/overview');
    $urlRouterProvider.otherwise('/login');
    $locationProvider.html5Mode(true);

    $stateProvider
        .state('base', {
            abstract: true,
            url: '',
            templateUrl: 'views/base.html'
        })
        .state('login', {
            url: '/login',
            parent: 'base',
            templateUrl: 'views/login.html',
            controller: 'LoginCtrl'
        })        
        .state('dashboard', {
            url: '/dashboard',
            parent: 'base',
            templateUrl: 'views/dashboard.html',
            controller: 'DashboardCtrl'
        })
        .state('collect', {
            url: '/collect',
            parent: 'dashboard',
            templateUrl: 'views/dashboard/collect.html',
            controller: 'CollectCtrl'
        })
        .state('overview', {
            url: '/overview',
            parent: 'dashboard',
            templateUrl: 'views/dashboard/overview.html'
        })
        .state('reports', {
            url: '/reports',
            parent: 'dashboard',
            templateUrl: 'views/dashboard/reports.html'
        });

}]);