angular.module('myApp',[])
	.controller('myCtrl',function($scope,$http){
		$scope.searching = false;
		$scope.searchPics = function(tagQuery){
			$scope.tagSearched = tagQuery;

			$scope.searching = true;
			$scope.results = false;
			$scope.resultsNone = false;

			setTimeout(function(){
				$scope.searching = false;
				if($scope.myForm.$valid) {
					console.log("Form is valid");

					var tag = $scope.tagSearched;
					var url = "https://api.instagram.com/v1/tags/"+tag+"/media/recent";
					$http({
						method: 'JSONP',
						url: url,
						params:{
							callback: 'JSON_CALLBACK',
							client_id: '7be53c459291486ead4916048ac434ad'
						}
					})
					.success(function(response){
						//console.log("success");
						$scope.tagQuery = '';
						$scope.myForm.$setPristine();
						$scope.myForm.$setUntouched();
						$scope.results = true;

						$scope.searchedItems = response.data;
						$scope.numItems = response.data.length;
						console.log($scope.searchedItems);
					})
					.error(function(error){
						console.log(error);
						$scope.resultsNone = true;
					});

				} else {
					console.log("Form is invalid");
					return false;
				}
			},1000);
				
		};
	});