angular.module('ionicSound')
    .controller('bookmarkCtrl', function($scope, $cordovaMedia, $ionicLoading, $rootScope) {


example.controller("bookmarkCtrl", function($scope, $cordovaMedia, $ionicLoading, $rootScope) {
    document.addEventListener("deviceready", onDeviceReady, false);
    $scope.bookmarkList = $rootScope.bookmarkList;
});
});