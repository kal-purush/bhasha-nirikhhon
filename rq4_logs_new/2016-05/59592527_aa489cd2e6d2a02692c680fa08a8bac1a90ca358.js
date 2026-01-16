var myApp = angular.module('myApp', [
    'ngRoute',
    'ui.bootstrap',
    'ngResource',
    'btford.socket-io']).
    config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){

        $routeProvider.when('/', {templateUrl: '/partials/projects/chat.html', controller: 'chatController'});
        
        //if no valid routes are found, redirect to /
        $routeProvider.otherwise({redirectTo: '/'});

        $locationProvider.html5Mode({enabled: true, requireBase: false});
    }])
    