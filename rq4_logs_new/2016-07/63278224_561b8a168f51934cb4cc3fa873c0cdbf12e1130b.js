angular.module('app').controller('mvProfileCtrl', function($scope, mvAuth, mvIdentity, mvNotifier){
	$scope.email = mvIdentity.currentUser.username;
	$scope.firstname = mvIdentity.currentUser.firstName;
	$scope.lastname = mvIdentity.currentUser.lastName;

	$scope.update = function(){
		var newUserData = {
			useranme: $scope.email,
			firstName: $scope.firstname,
			lastName: $scope.lastname
		};

		if($scope.password && $scope.password.length > 0) {
			newUserData.password = $scope.password;
		}

		mvAuth.updateCurrentUser(newUserData).then(function(){
			mvNotifier.notify('Your account has been successfully updated!');
		}, function(err){
			mvNotifier.error(err);
		});
	}
});