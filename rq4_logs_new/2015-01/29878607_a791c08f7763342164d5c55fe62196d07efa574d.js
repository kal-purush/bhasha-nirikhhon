'use strict';

/**
 * @ngdoc function
 * @name wordCannonApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the wordCannonApp
 */
angular.module('wordCannonApp')
  .controller('NewPlayerCrtl', function ($scope, $q, playerFactory, letterFactory) {

  	$scope.player = null;
  	// function generateTiles(n){
  	// 	letterFactory.getLetterRand(n)
 		// .then(function(data)
 		// {
 		// 	//data returned from getLetterRand is letters array from letterHelper
 		// 	letters = data;
 		// 	console.log(letters);
 		// });
  	// }

	$scope.setPlayer = function(){
   		$scope.player = playerFactory.addPlayer($scope.tempName, $scope.tempHealth);
   		letterFactory.getLetterRand(8).then(function(data,error)
   		{
   			$scope.player = playerFactory.setTiles(data);
   			console.log($scope.player);
   			console.log($scope.player.tiles.letter)
   		})
	}
  });