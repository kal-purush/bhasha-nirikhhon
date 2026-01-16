var myapp = angular.module("myapp", []);
myapp.controller("LoginController", function($scope) {
    	$scope.user = {
    		username: "",
    		password: "",
    		fullName:  function() {
    	         var userObject;
    	         userObject = $scope.user;
    	        return userObject.username + " " + userObject.password;
    	      }
    	};
            
});