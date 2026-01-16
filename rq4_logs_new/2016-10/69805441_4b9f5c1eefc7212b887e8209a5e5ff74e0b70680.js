// create the module and name it CognizantUIApp
var CognizantUIApp = angular.module('CognizantUIApp', ['ngRoute']);


// configure our routes
CognizantUIApp.config(function ($routeProvider) {

    $routeProvider

        // route for the home page
        .when('/', {
            templateUrl: 'pages/Home.html',
            controller: 'mainController'
        })

        // route for the contact page
        .when('/Catalog', {
            templateUrl: 'pages/Catalog.html',
            controller: 'mainController'
        })

                // route for the about page
        .when('/Developer', {
            templateUrl: 'pages/Developer.html',
            controller: 'mainController'
        });
});

// create the controller and inject Angular's $scope
CognizantUIApp.controller('mainController', function ($scope, $http) {
//    var datai = [];
//    localStorage.setItem("dataitems", datai);
//    for (var i = 0; i < 1347; i++) {
//        $http({
//            method: 'GET',
//            url: 'http://cors.io/?http://hackerearth.0x10.info/api/ctz_coders?type=json&query=list_submissions&page=' + i,
//        })
//   .success(function (result) {
//       var newdatai = localStorage.getItem("dataitems");
//       localStorage.setItem("lastname", newdatai + result.elements);;
//   })
//.error(function (error) { });
//    }

    console.log(localStorage.getItem("dataitems"));

    $scope.loading = false;

    $scope.SearchQuery = function () {
        $scope.loading = true;
        $http({
            method: 'GET',
            url: 'http://hackerearth.0x10.info/api/ctz_coders?type=json&query=list_submissions&page=1',
        })
           .success(function (result) {
               $scope.entries = result.websites;
               console.log(result.websites);
               window.location.href = '#/Catalog';
               $scope.loading = false;
           })
       .error(function (error) { });
    };
});
