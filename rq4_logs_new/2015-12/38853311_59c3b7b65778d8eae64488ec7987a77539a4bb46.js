angular.module('versusApp')
    .controller('mainController',
        function($scope, $rootScope, $location) {
        'use strict';
    
        if (!$rootScope.player) {
            $location.path('/');
        }
    
    });