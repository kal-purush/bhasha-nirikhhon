///<reference path="app.d.ts" />
var thymioApp = angular.module('thymioApp', []);

thymioApp.controller('ThymioCtrl', ['$scope', function($scope) {
	$scope.statusx = "Not Connected";
	$scope.devices = [{ 'displayName': 'not connected' }];
	$scope.connectd = function() {
		console.debug("Button pressed");
		chrome.usb.getUserSelectedDevices(
			{
				'multiple': false,
				'filters': [{ 'vendorId': 0x0617 }]
			},
			function(selected_devices) {
				console.debug("In function");
				console.debug(selected_devices.length)
				// $scope.devices = selected_devices;
				$scope.statusx = "Connected";
			}
		);
		console.debug("End of process");
	};
}]);