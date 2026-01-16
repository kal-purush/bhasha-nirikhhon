import * as angular from "angular";

import "angular-material/angular-material.css!";
import "angular-material";
import "normalize.css";


import registerMySideNav from "my-app/my-side-nav/my-side-nav";

let myAppModule = angular.module("myApp", ["ngMaterial"]);

registerMySideNav(myAppModule);

class MyAppController {

    constructor(private $scope, private $timeout, private $mdSidenav, private $log) {

	}

	public toggleLeft() {
		let navId = "left";
		this.$mdSidenav(navId)
					.toggle()
					.then(function() {
						this.$log.debug("toggle " + navId + " is done");
					});
	}
}



myAppModule.controller("MyAppControler", MyAppController);
myAppModule.directive('myApp', function(): angular.IDirective {
	return {
		restrict: 'E',
		templateUrl: './my-app/my-app.html',
		controller: 'MyAppControler',
		controllerAs: 'vm'
	};
});



// Bootstrap angular manually on whole document
angular.element(document).ready(function() {
    angular.bootstrap(document, ["myApp"]);
});



