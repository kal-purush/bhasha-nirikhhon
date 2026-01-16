declare var Swipe: any;

/* global angular */
(function () {
	'use strict';

	/**
	 * Directive: skySwipeGallery
	 * Directive for our integration of the `swipe.js` gallery
	 * (which is a manual dependency)
	 *
	 * Uses skyShow to show either bullets or controls based on touch-capabilities.
	 *
	 * This gallery relies on styling from swipe.scss
	 *
	 **/

	angular.module('skySwipe').directive('skySwipe',skySwipe);

	skySwipe.$inject = ['$timeout'];

	function skySwipe($timeout) {
		var directive = {
			restrict:'E',
			transclude:true,
			scope:true,
			templateUrl:'/sky-swipe/sky-swipe.template.html',
			link:link
		};
		function link(scope,element,attributes) {
			element.addClass('swipe-gallery');
			scope.images=[];

			/* Build scope.images from the used elements */
			angular.forEach(element.find('div'),function(element,key) {
				scope.images.push({
					active:false
				});
			});

			/* Updating scope-values to startout at the first one. */
			scope.images[0].active=true;
			scope.current=1;

			/* Defining the gallery, and the callback method that runs on each swipe */
			var gallery = new Swipe(element[0], {
				continuous: false,
				callback:function(which) {
					/**
					 * Whenever the image is changed (swiped or via controls), update
					 * `scope.current`, and set the `active` property on the specific
					 * image.
					 *
					 * Wrapped in $timeout to deal with possible running $apply
					 *
					 **/
					$timeout(function() {
						angular.forEach(scope.images, function(item,key) {
							if (which == key) {
								scope.images[key] = {
									active:true
								};
							} else {
								scope.images[key] = {
									active:false
								};
							}
						});

						scope.current=which+1;

					},0);
				}
			});

			/* Assigning swipe.js' slide-, next- and prevmethods to the scope */
			scope.slide=function(which) {
				gallery.slide(which);
			};
			scope.next=function() {
				gallery.next();
			};
			scope.prev=function() {
				gallery.prev();
			};
		}

		return directive;

	}

})();