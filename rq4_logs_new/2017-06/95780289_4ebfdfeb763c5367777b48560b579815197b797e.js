var myApp = angular.module('myApp', []);

//this is used to parse the profile
function url_base64_decode(str) {
  var output = str.replace('-', '+').replace('_', '/');
  switch (output.length % 4) {
    case 0:
      break;
    case 2:
      output += '==';
      break;
    case 3:
      output += '=';
      break;
    default:
      throw 'Illegal base64url string!';
  }
  return window.atob(output); //polyfill https://github.com/davidchambers/Base64.js
}

myApp.controller('UserCtrl', function ($scope, $http, $window,$rootScope) {
	$rootScope.defaultView='loginForm';
	  $scope.user = {username: '', password: ''};
	  $scope.message = '';
	  $scope.submit1 = function () {
	    $http
	      .post('/authenticate', $scope.user)
	      .success(function (data, status, headers, config) {
	    	  
	        $window.sessionStorage.token = data.token;
	        $scope.token=data.token;
	        $scope.message = 'Welcome';
	        $rootScope.defaultView='token';
	      })
	      .error(function (data, status, headers, config) {
	        // Erase the token if the user fails to log in
	    	  $window.location.assign('errorAuthenticate');
	        delete $window.sessionStorage.token;

	        // Handle login errors here
	        $scope.message = 'Error: Invalid user or password';
	      });
	  };
	
	  $scope.submitToken= function(){
		  //$rootScope.defaultView='hello';
		  $http
	      .get('/getUserEffortProjectWise', $scope.user,$scope.token);
	  };
  $scope.logout = function () {
    $scope.welcome = '';
    $scope.message = '';
    $scope.isAuthenticated = false;
    delete $window.sessionStorage.token;
  };

  $scope.callRestricted = function () {
    $http({url: '/api/restricted', method: 'GET'})
    .success(function (data, status, headers, config) {
      $scope.message = $scope.message + ' ' + data.name; // Should log 'foo'
    })
    .error(function (data, status, headers, config) {
      alert(data);
    });
  };
  });



myApp.factory('authInterceptor', function ($rootScope, $q, $window) {
  return {
    request: function (config) {
      config.headers = config.headers || {};
      if ($window.sessionStorage.token) {
        config.headers.Authorization = 'Bearer ' + $window.sessionStorage.token;
      }
      return config;
    },
    responseError: function (rejection) {
      if (rejection.status === 401) {
        // handle the case where the user is not authenticated
      }
      return $q.reject(rejection);
    }
  };
});

myApp.config(function ($httpProvider) {
  $httpProvider.interceptors.push('authInterceptor');
});