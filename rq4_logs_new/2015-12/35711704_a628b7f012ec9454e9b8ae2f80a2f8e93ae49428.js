(function() {
	'use strict';

	var app = angular.module('app', []);

	app.controller('testCtrl', ['$scope', function($scope) {
		$scope.name = '1111';
	}]);

	app.directive('bindDirect', ['$rootScope', function($rootScope) {
		// Runs during compile
		return {
			scope: {
				name: '&'
			},
			link: function($scope, iElm, iAttrs, controller) {
				console.log($scope.name());
			}
		};
	}]);
})();