(function() {
	'use strict';
	
	/**
	 * SkyAgo Directive
	 * 
	 * Usage: <sky-ago date-time="2015-09-01T10:33:00+02:00" interval="60"></sky-ago>
	 * 
	 * dateTime: string with date (default now)
	 * interval: seconds between updates (default 0, off)
	 * 
	 */
	
	angular.module('skyAgo').directive('skyAgo',skyAgoDirective);
	
	skyAgoDirective.$inject = ['$interval'];
	
	function skyAgoDirective($interval) {
		var directive = {
			restrict: 'E',
			scope: {},
			link: link
		};
		
		function timeElapsed(time:Date):string {
			var ms = new Date().getTime() - time.getTime();
			var sec = Math.round(ms / 1000);
			var min = Math.round(sec / 60);
			var hr = Math.round(min / 60);
			var day = Math.round(hr / 24);
			if (ms < 0 || sec < 20) {
				return 'lige nu';
			} else if (sec < 90) {
				return sec + ' sekunder siden';
			} else if (min < 45) {
				return min + ' minutter siden';
			} else if (min < 90) {
				return '1 time siden';
			} else if (hr < 24) {
				return hr + ' timer siden';
			} else if (hr < 36) {
				return 'en dag siden';
			} else if (day < 30) {
				return day + ' dage siden';
			} else {
				return '';
			}
		};

		function link(scope, element, attrs) {
			
			var date = ('dateTime' in attrs) ? new Date(attrs.dateTime) : new Date();
			
			element.html(timeElapsed(date));

			if('interval' in attrs && attrs.interval > 0) {
				$interval(() => {
					element.html(timeElapsed(date));
				}, attrs.interval*1000);
			}
		}		
		return directive;
	}
	
})();