/* global angular */
(function () {
	'use strict';

	/*
	 * Directive: skyShow
	 *
	 * Use this directive to add view-conditions based on Modernizr feature tests, or negation-test (!str);
	 *
	 * Use like <div sky-show="touch"></div> or <div sky-show="!touch"></div>
	 *
	 */

	angular.module('skyShow').directive('skyShow',skyShow);

	skyShow.$inject = ['Modernizr'];

	function skyShow(Modernizr) {
		var directive = {
			restrict:'EA',
			link:link
		};
		function link(scope,element,attributes) {
			var conditionPassed = true;
			if (attributes.skyShow.indexOf('!') != -1) {
				conditionPassed = !Modernizr[attributes.skyShow.slice(1)];
			} else {
				conditionPassed = Modernizr[attributes.skyShow];
			}
			if (!conditionPassed) {
				element[0].style.display='none';
			}
		}

		return directive;
	}

})();