'use strict';

// Define the `accountSearch` module
angular.module('accountSearch', [
  'ngRoute'
]);


// Register `accountSearch` component, along with its associated controller and template
angular.
  module('accountSearch').
  component('accountSearch', {
    templateUrl: 'app/account-search/account-search.template.html',
    controller: ['$http', '$routeParams',
      function AccountSearchController($http, $routeParams) {
        //TODO
      }
    ]
  });