var app = angular.module('Leaderboard', ['ngAnimate']);


(function () {

  
angular.module('Leaderboard').controller('LeaderboardController', function($scope, $http) {
  $scope.Teams = [  ];

  $http.get('Teams.json').
    success(function(data, status, headers, config) {
      $scope.Teams = data.Teams;
    }).
    error(function(data, status, headers, config) {
      $scope.Teams = [
	  {
	  	'name': 'Teams not loaded',
	  	'url:': 'www',
	  }, ];
    });

});



})();


