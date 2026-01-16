'use strict';

/**
 * @ngdoc function
 * @name mtViewApp.controller:EntitysearchCtrl
 * @description
 * # EntitysearchCtrl
 * Controller of the mtViewApp
 */
angular.module('mtViewApp')
  .controller('EntitysearchCtrl', [ '$scope','$routeParams','MTservice',function ($scope,$routeParams,MTservice) {
   
  	 var entityName = $routeParams.name;
  	 var entityUri = $routeParams.searchEntity;

  	 if(!entityName || !entityUri){
  	 	$location.path('/');
  	 	$location.replace();
  	 }
  	 $scope.entityName = entityName;

  	 MTservice.searchEntity(entityUri);

  }]);