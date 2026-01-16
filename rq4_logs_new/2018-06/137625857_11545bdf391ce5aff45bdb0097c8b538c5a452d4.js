var app = angular.module("app", ["ui.router"]);

app.config(function($stateProvider,$urlRouterProvider) {
	$stateProvider
	.state('Home', {
		url: '/home',
		templateUrl: 'home.html',
		controller : 'subscribeController'

	})

	.state('About', {
		url : '/about',
		templateUrl : 'about.html',
		controller : 'subscribeController'
			} )

	.state('Service', {
		url : '/services',
		templateUrl: 'services.html',
		controller : 'subscribeController'
	})

	.state('thankyou',{
		url : '/thankyou/:a',
		templateUrl : 'thankyou.html',
		controller : 'thankyou'
	})
	.state('feedback', {
		url:'/feedback/:f',
		templateUrl: 'feedback.html',
		controller:'feedback'
	});

	$urlRouterProvider.otherwise('/home');

});

app.controller('thankyou', function($scope, $stateParams){

	$scope.email = $stateParams.a;
});

app.controller('subscribeController', function($scope, $state){

		
		$scope.error_message='';
		$scope.email = '';

		$scope.validEmail = function(input){

			var emailPattern = /^[a-zA-z]+[a-zA-Z0-9._]+@+[0-9a-zA-Z]+\.+[a-zA-z]{2,3}$/;
	

			if(!input.match(/^[a-zA-z]+[a-zA-Z0-9._]+@+[0-9a-zA-Z]+\.+[a-zA-z]{2,3}$/)){
				$scope.error_message = "Please enter a valid email id";
				}

			else{
				$state.go('thankyou',{a:$scope.email});
				}
};

		$scope.feedbackObj = {};
		$scope.error = {};

		$scope.validFeedBackForm = function(input){

			if (!input.name) {
				$scope.error = {};
				$scope.error.name = "Please enter a valid Name";
				
			}
			else if (!input.email||!(input.email.match(/^[a-zA-z]+[a-zA-Z0-9._]+@+[0-9a-zA-Z]+\.+[a-zA-z]{2,3}$/))) {
				$scope.error = {};
				$scope.error.email = "Please enter a valid Email Id";
				
			}

			else if (!input.message) {
				$scope.error = {};
				$scope.error.msg = "Message field can not be empty";
				
			}

			else {
				$state.go('feedback', {f:$scope.feedbackObj.message});
				$scope.feedbackObj = {};
				$scope.error = {};
			}

		};


});


app.controller('feedback', function($scope, $stateParams){

	$scope.feedback_message = $stateParams.f;
});