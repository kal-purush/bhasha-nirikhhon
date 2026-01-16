'use strict';

const angular = require('angular');
const baseURL = 'http://localhost:3000/api/pic';

var picApp = angular.module('picApp', []);

picApp.controller('MainController',
  ['$scope', '$http', function($scope, $http) {

  $scope.pics = [];
  $scope.get = function() {
    $http({
      url: baseURL,
      method: 'GET'
    })
      .then(function(res) { // eslint-disable-line
        $scope.pics = res.data;
        console.log(res);
        console.log('got those pics you wanted');
      }, function(err) { // eslint-disable-line
        console.log(err);
        console.log('really sorry, could not get those pictures');
      });
  };

  $scope.post = function(newPic) {
    $http({
      url: baseURL,
      data: newPic,
      method: 'POST'
    })
      .then(function(res) { // eslint-disable-line
        $scope.pics.push(newPic);
        $scope.newPic = null;
        console.log('success posting!');
        console.log(res);
      }, function(err) { // eslint-disable-line
        console.log('could not post!');
        console.log(err);
      });
  };
}]);