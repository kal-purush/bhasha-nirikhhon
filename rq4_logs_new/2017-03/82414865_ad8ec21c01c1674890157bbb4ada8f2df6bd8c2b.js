angular.module('myApp').controller('SideNavCtrl', ['$scope', '$http', '$location', function($scope, $http, $location){
	console.log('SideNavCtrl working...');
	$scope.user = {name:'test', password:'test'};
	$scope.authUser = function(usr) {
		console.log('');
		console.log('auth user working...');

	}


	$scope.openInventory = function(){
		console.log('opened inventory');
		$location.path('/epos/inventory');

	}


	$scope.openMenu = function(){
		console.log('opened the menu...');
		$location.path('/epos/menu');
	}

	$scope.openOrders = function() {
		console.log('opened the active orders...');
		$location.path('/epos/orders');
	}

	$scope.openCustomers = function() {
		console.log('opened the customers directory..');
		$location.path('/epos/customers');
	}

	$scope.openHome = function() {
		console.log('opened the home/admin page');
		$location.path('/epos/home');

	}

}]);