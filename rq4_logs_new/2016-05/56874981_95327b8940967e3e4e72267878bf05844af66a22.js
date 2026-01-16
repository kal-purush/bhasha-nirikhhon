/**
 * Created by JonDai on 2016/5/6.
 */
'use strict'
angular.module('jondaiApp')
  .config(function ($routeProvider) {
    $routeProvider
      .when('/music',{
        template: '<music></music>'
      })
  });