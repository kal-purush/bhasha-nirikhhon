var app = angular.module('flapperNews', []);

app.controller('MainCtrl',  
	['$scope',
	function($scope){
		$scope.test = "Flapper News";
		$scope.posts = [
			{title: 'post1', upVotes: 5},
			{title: 'post2', upVotes: 2},
			{title: 'post3', upVotes: 15},
			{title: 'post4', upVotes: 9},
			{title: 'post5', upVotes: 4}
		];
		$scope.addPost = function(){
			//$scope.posts.push({title: 'A new post', upVotes: 0});
			if(!$scope.title || $scope.title === ""){
				return;
			};
			$scope.posts.push(
			{
				title: $scope.title,
				link: $scope.link, 
				upVotes: 0
			});
			$scope.title = "";
			$scope.link = "";
		};
		$scope.incrementUpvotes = function(post){
			post.upVotes += 1;
		};
	}
	]);