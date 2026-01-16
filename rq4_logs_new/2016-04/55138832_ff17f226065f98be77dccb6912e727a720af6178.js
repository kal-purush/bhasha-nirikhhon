var homeApp = angular.module('homeApp', []);
homeApp.config(['$routeProvider', '$locationProvider',
    function ($routeProvider, $locationProvider) {
        $routeProvider.when('/catlocation', {
            templateUrl: 'pages/location/catlocation.html',
            controller: 'LocationController'
        }).when('/insertlocation', {
            templateUrl: 'pages/location/insertLocation.html',
            controller: 'LocationController'
        }).when('/updatelocation/:id', {
            templateUrl: 'pages/location/updatelocation.html',
            controller: 'LocationController'
        }).when('/userinfo', {
            templateUrl: 'pages/usermanager/usermanager.html',
            controller: 'UserController'
        }).when('/insertuser', {
            templateUrl: 'pages/usermanager/insertUser.html',
            controller: 'UserController'
        }).when('/updateuser/:id', {
            templateUrl: 'pages/usermanager/updateUser.html',
            controller: 'UserController'
        }).otherwise({
            redirectTo: '/'
        });

        //        $locationProvider.html5Mode(true); //Remove the '#' from URL.
    }
]);
homeApp.controller('HomeController', function ($scope, $rootScope) {
    //config cau hinh duong link server
    $rootScope.appUrl = 'http://localhost/DemoRestful/public/';
    //cau hinh duong link local
    $rootScope.localUrl = 'http://localhost:8383/DemoAngularJS/';
    $scope.logout = function () {
        window.location = $rootScope.localUrl + 'index.html';
    };
});