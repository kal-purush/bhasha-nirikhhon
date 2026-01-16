/// <reference path='../../typings/angularjs/angular.d.ts' />
/// <reference path='../../typings/angular-ui-router/angular-ui-router.d.ts' />

module sm.config {
	angular.module('sm.config', [])
		.config(routeConfiguration);
		
	routeConfiguration.$inject = ['$state'];
	function routeConfiguration($state: ng.ui.IStateService): void {
		'use strict';
	}
	
	routes.$inject = ['$stateProvider'];
	function routes($stateProvider: ng.ui.IStateProvider): void {
		$stateProvider
			.state('shotList', {
				url: '/shotList',
				templateUrl: 'views/shotList/shotList.html',
			});
	}
}