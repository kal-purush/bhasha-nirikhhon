angular.module('beerApp', ['ngCookies','ngRoute'])
	.controller('userController',['$scope', '$cookies','Network',userController])
	.controller('beerController',['$scope','$q','$location','Network',beerController])
	.service('Network',Network)
	.directive('login', function() {
		return {
		    restrict: 'E',
		    controller: 'userController',
		    templateUrl: './templates/login.html',
			}
		}
	)
	.directive('description', function() {
		return {
		    restrict: 'E',
		    templateUrl: './templates/description.html',
		    controller: 'beerController'
			}
		}
	)
	.directive('cart', function() {
		return {
		    restrict: 'E',
		    templateUrl: './templates/cart.html'
			}
		}
	)
	.config(['$routeProvider',function($routeProvider) {
	  $routeProvider
	   .when('/', {
	    templateUrl: './templates/beerView.html'
	    //controller: 'beerController'
	    
	  })
	  .when('/thankyou', {
	    templateUrl: './templates/thankyouView.html',
	  })
	  .otherwise({
        redirectTo: '/'
      });
	}])
