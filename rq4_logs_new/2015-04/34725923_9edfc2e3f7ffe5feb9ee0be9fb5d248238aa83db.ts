/* global angular */
(function () {
	'use strict';

	angular.module('skyWindowEvents',[]);


	/**
	 * The run-phase added so the service is always instanciated when the 
	 * module is loaded. (and not wait untill the service is injected)
	 * We need it to happen before the window-events are actually run.
	 * 
	 */
	angular.module('skyWindowEvents').run(['skyWindowEvents',angular.noop]);

})();