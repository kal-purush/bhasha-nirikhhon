'use strict';

/**
 * @ngdoc service
 * @name parkattackgithubioApp.Auth
 * @description
 * # Auth
 * Factory in the parkattackgithubioApp.
 */
angular.module('parkattackgithubioApp')
  .factory('Auth', function ($firebaseAuth, $rootScope) {
    var ref = new Firebase('https://parkattack.firebaseio.com');
    return $firebaseAuth(ref);
  });