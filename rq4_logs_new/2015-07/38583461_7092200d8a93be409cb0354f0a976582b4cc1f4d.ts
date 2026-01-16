/// <reference path='../../../typings/angularjs/angular.d.ts' />
/// <reference path='./adshotView.ts' />

module sm.views.shot {
	angular.module('sm.views.adshot', [])
		.directive('smAdshotView', sm.views.adshotView.adshotView);
}