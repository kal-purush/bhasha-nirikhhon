(function () {

  'use strict';

  angular.module('app')

  .controller('AutoCtrl', function ($scope, $http, HEROKU, $stateParams) {

    $scope.autoBuild = [];


  });



$scope.autoBuild = function (total_price) {

  $http.post(HEROKU.URL + 'computer/build', max_price, HEROKU.CONFIG)
    .success( function () {



    })
}



































}());