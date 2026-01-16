angular.module('myApp', ['ngRoute'])
    .controller('welcomeController', ['$scope', function($scope){
            
	}])
	.config(['$routeProvider', function($routeProvider){
        $routeProvider
        .when('/', {
            templateUrl: 'views/main.html',
            controller: 'mainController'
        })
        .otherwise({
            redirectTo: '/'
        });

}]);