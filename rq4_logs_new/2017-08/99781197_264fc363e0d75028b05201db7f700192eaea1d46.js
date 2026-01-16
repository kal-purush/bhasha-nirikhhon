'use strict';

//var appContext = "http://localhost:8080" ;
var appContext = "" ;
/* App Module */

var dreamApp = angular.module('dreamApp', [
    'ngRoute',
    'ngResource'
]);


dreamApp.config(['$routeProvider', '$locationProvider',   
    function($routeProvider, $locationProvider) {

        $routeProvider.
                when('/', {
                    templateUrl: 'partials/customers.html',
                    controller: 'costomerCtl'
                });

        $locationProvider.html5Mode(false).hashPrefix('!');
    } ]);

 