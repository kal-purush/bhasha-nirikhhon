/**
 * New node file
 */

myNotesApp.factory("TokenInterceptor", function($q, $window, AuthenticationService) {
	return {
		request: function(config) {
			config.headers = config.headers || {};
			if($window.sessionStorage.token) {
				config.headers.Authorization = $window.sessionStorage.token + "sdafas";
			}
			return config;
		},
		
		requestError: function(rejection) {
	        return $q.reject(rejection);
	     },
	 
		
		response: function(response) {
			return response || $q.when(response);
		},
		
	   responseError: function(rejection) {
	        if (rejection != null && rejection.status === 401 && ($window.sessionStorage.token || AuthenticationService.isAuthenticated)) {
	                delete $window.sessionStorage.token;
	                AuthenticationService.isAuthenticated = false;
	                $location.path("/login");
	            }
	            return $q.reject(rejection);
	        }
	}
});