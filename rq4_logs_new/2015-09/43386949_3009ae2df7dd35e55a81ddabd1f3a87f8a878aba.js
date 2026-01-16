'use strict';

angular.module('noblogApp')
.filter('reverse', function() {
	return function(items) {
		return items.slice().reverse();
	};
})
.filter('idToTime', function() {
	return function(objectID) {
		var timestamp = parseInt(('' + objectID).substr(0,8), 16);
		var time = new Date(timestamp * 1000);
		var now = new Date();
		var diff = (now - time) / 1000;

		if (diff < 60) {
			return Math.floor(diff) + 'sec';
		} else if (diff < 60 * 60) {
			return Math.floor(diff / 60) + 'min';
		} else if (diff < 60 * 60 * 24) {
			return Math.floor(diff / 360) + 'hours';
		} else if (time.getFullYear() === now.getFullYear()) {
			return (time.getMonth() + 1) + '/' + time.getDate();
		}

		return new Date(timestamp * 1000).toDateString();
	};
})
.controller('PostCtrl', function($scope, Post) {
	$scope.posts = [];

	Post.index()
	.then(function(data) {
		$scope.posts = data;
	});

	$scope.removePost = function(post) {
		if(! post) { return; }
		Post.remove(post);
	};
})
.controller('PostNewCtrl', function($scope, $location, Post) {
	$scope.post = {};
	$scope.createPost = function() {
		var post = $scope.post;
		Post.create(post)
		.then(function() {
			$location.path('/post');
		});
	};
})
.controller('PostViewCtrl', function($scope, $location, $routeParams, Post) {
	$scope.post = {
		title: 'Error',
		content: 'cannot find post',
		comments: []
	};

	Post.get($routeParams.id)
	.then(function(data) {
		$scope.post = data;
	});

	$scope.isAdmin = function() {
		// TODO
		return true;
	};

	$scope.isMyPost = function() {
		// TODO
		return false;
	};

	$scope.removePost = function() {
		Post.remove($scope.post)
		.then(function() {
			$location.path('/post');
		});
	};
})
.controller('PostEditCtrl', function($scope, $location, $routeParams, Post) {
	$scope.post = {};

	Post.get($routeParams.id)
	.then(function(data) {
		$scope.post = data;
	});

	$scope.cancel = function() {
		$location.path('/post/' + $scope.post._id);
	};

	$scope.updatePost = function() {
		var post = $scope.post;
		Post.update(post)
		.then(function() {
			$location.path('/post/' + $scope.post._id);
		});
	};
});