(function () {
	'use strict';

	angular
		.module('app')
		.controller('NavController', ['$scope', '$state', Controller]);

	function Controller($scope, $state) {

		$scope.currState = $state.current.name;

	}


})();

