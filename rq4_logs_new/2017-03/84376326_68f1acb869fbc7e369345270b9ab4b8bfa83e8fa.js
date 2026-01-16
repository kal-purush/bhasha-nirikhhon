'use strict';

var templateApp = angular.module('templateApp', ['ngRoute', 'ngMaterial', 'ngMessages', 'ngResource']);

templateApp.config(function($mdIconProvider) {

	$mdIconProvider
		.defaultIconSet("images/svg/avatars.svg", 128)
		.icon("menu", "/images/svg/menu.svg", 24)
		.icon("share", "images/svg/share.svg", 24)
		.icon("google_plus", "images/svg/google_plus.svg", 24)
		.icon("hangouts", "images/svg/hangouts.svg", 24)
		.icon("twitter", "images/svg/twitter.svg", 24)
		.icon("phone", "images/svg/phone.svg", 24);
});

templateApp.controller('indexController', ['$scope', '$mdSidenav', '$mdBottomSheet', function($scope, $mdSidenav, $mdBottomSheet) {

	$scope.toggleList = function toggleList() {
		$mdSidenav('left').toggle();
	}


}]);