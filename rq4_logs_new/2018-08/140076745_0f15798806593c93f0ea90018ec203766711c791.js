var app = angular.module("app", ['ui.router','ui.bootstrap']);

app.config(function ($stateProvider, $urlRouterProvider) {

    $urlRouterProvider.when("", "/login");

    $stateProvider
        .state("home", {
            url: "/home",
            templateUrl: "home.html"
        })
        .state("login", {
            url:"/login",
            templateUrl: "login.html"
        })
        .state("home.article", {
            url:"/article?page&size&total&startAt&endAt&status&type&id",
            templateUrl: "article.html"
        })
        .state("home.new", {
            url:"/new?id",
            templateUrl: "new.html"
        });
});