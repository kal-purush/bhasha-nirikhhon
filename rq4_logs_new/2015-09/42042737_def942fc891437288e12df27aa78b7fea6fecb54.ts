(function() {
	'use strict';

	/**
	 * Angular-component for multi-line text-ellipsis
	 *
	 * <sky-ellipsis>Lots of text goes here</sky-ellipsis>
	 * 
	 * Dont put styles on this element - use wrapper to set height, padding, etc. 
	 *
	 **/

	angular.module('skyEllipsis').directive('skyEllipsis', skyEllipsisDirective);

	skyEllipsisDirective.$inject = ['$rootScope'];

	function skyEllipsisDirective($rootScope) {
		var directive = {
			restrict: 'E',
			link: link
		};

		function link(scope, element) {
			// These styles must be present
			element.css({
				'display': 'block',
				'height': '100%',
				'position': 'relative'
			});
		
			// Wrap each word in a span
			var wrapped = element[0].innerHTML.replace(/[\w,'/.]+/g, "<span>$&</span>");

			function calculate() {
				// Cache the maxHeight of the box
				var maxHeight = element[0].clientHeight;
				var overflowingElements = [];
				var lastElement;

				// (re) set the content to the wrapped content
				element[0].innerHTML = wrapped;

				// Loop through all the spans
				angular.forEach(element.find('span'), function(word) {
					//if it is not visible at calc-time, push into hideElement-array
					if (maxHeight < (word.offsetTop + word.offsetHeight)) {
						overflowingElements.push(word);
					} else {
						//otherwise - keep reference to last-word (for replacing)
						last = word;
					}
				});

				// Hide overflowing elements
				if (overflowingElements.length) {
					angular.forEach(overflowingElements, function(ele) {
						ele.style.display = 'none';
					});
				}

				// Replace last visible element with ...
				if (lastElement) {
					angular.element(lastElement).replaceWith('<span class="dotdotdot">...</span>');
				}
			}
		};

		return directive;
	}
})();