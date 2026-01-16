/// <reference path="../../../typings/tsd.d.ts" />

module myApp {
	class myDirective implements angular.IDirective {
		public restrict = 'AE';
		public template = "<p>I'm a directive!</p>";
		
		constructor(private $http: angular.IHttpService) { }
		
		public link: angular.IDirectiveLinkFn = (scope: angular.IScope, element: angular.IAugmentedJQuery, attrs: angular.IAttributes) => {
		element.addClass('someClass');
		}
		
		static factory() {
		var directive = ($http) => {
			return new myDirective($http);
		};
		directive.$inject = ['$http'];
		return directive;
		}
	}
	angular.module(appModuleId).directive('myDirective', myDirective.factory());
}