/**
 * Created by Steve on 10/25/2015.
 */
var app = angular.module("AppName", []);

app.config(function ($routeProvider){
    $routeProvider
        .when('/' , {
            controller:'HomeController',
            templateUrl: '../views/home.html'
        })
        .when('/outbox/:id' , {
            controller:'EmailController' ,
            templateUrl:'../views/email.html'
        })
        .otherwise({
            redirectTo: '/'
        });
});