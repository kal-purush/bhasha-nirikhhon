var watchApp = angular.module('watchApp', []);

watchApp.controller('watcherController', ['$scope', '$interval', function($scope, $interval) {
    $scope.counter = 100;

    $scope.$watch("counter", function (newValue, oldValue) {
       console.log('oldValue = ' + oldValue + ', newValue = ' + newValue);
    });

    $interval(function () {
        $scope.counter++;
    }, 1000)

    console.log($scope.$$watchers)
}]);