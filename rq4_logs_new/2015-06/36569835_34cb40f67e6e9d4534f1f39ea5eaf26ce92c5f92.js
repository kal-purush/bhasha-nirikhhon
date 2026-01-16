'use strict';

angular.module('ClazzManager.login', ['ngRoute'])

.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/login', {
    templateUrl: 'views/v_login/login.html',
    controller: 'LoginCtrl'
  });
}])

.controller('LoginCtrl', [function() {
              
}]);