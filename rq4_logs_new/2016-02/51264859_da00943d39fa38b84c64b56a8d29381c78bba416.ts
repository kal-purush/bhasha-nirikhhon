module app {
    class Config {
        constructor($routeProvider: ng.route.IRouteProvider) {
            $routeProvider
                .when("/", {
                    templateUrl: "/posts/list.html",
                    controller: "PostsCtrl as vm"
                })
                .when("/edit/:id", {
                    templateUrl: "/posts/edit.html",
                    controller: "PostEditCtrl as vm"
                })
                .when("/add", {
                    templateUrl: "/posts/add.html",
                    controller: "PostAddCtrl as vm"
                })
                .otherwise({ redirectTo: '/' });
        }
    }
    Config.$inject = ['$routeProvider'];

    var mainApp = angular.module('testApp', ['ngRoute']);
    mainApp.config(Config);
}