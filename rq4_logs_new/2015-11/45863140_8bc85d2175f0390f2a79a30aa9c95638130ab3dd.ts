/// <reference path="../typings/tsd.d.ts" />
module dBApp {
	angular
		.module('dBApp')
		.config(["$urlRouterProvider", "$facebookProvider", ($urlRouterProvider: angular.ui.IUrlRouterProvider, $facebookProvider) => {
			$urlRouterProvider.otherwise("/");
			$facebookProvider.setAppId(421032427990165);
			$facebookProvider.setPermissions("publish_actions");
			$facebookProvider.setVersion("v2.4");
		}])
		.run(['$rootScope', function($rootScope) {
			$rootScope.facebookAppid = '[421032427990165]';

			(function(d) {
				var js,
					id = 'facebook-jssdk',
					ref = d.getElementsByTagName('script')[0];

				if (d.getElementById(id)) {
					return;
				}

				js = d.createElement('script');
				js.id = id;
				js.async = true;
				js.src = "//connect.facebook.net/en_US/sdk.js";

				ref.parentNode.insertBefore(js, ref);

			} (document));

		}]);
}