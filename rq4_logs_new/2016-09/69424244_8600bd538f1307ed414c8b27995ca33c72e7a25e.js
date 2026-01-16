
	

	var demo = angular.module('demo',[]);
demo.controller('myc',function($scope,$http) {
		$http.get('country_codes.json').success(function(response)
		{

			$scope.data=response;
		});




	// body...
});

