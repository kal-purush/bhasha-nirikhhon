(function(){
    var app = angular.module('ShopAdmin', ['ngRoute']);

    app.config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){
        $locationProvider.html5Mode({enabled: true, requireBase: false});
        $routeProvider
            .when('/', {templateUrl: '/customers.html'})
            .when('/customers', {templateUrl: '/customers.html'})
            .when('/products', {templateUrl: '/products.html'});
    }]);

    app.controller('ApplicationController', ['$location', function($location){
        this.hello = 'Hi, I am Angular.js';
        this.currentMenu = 'customers';

        this.changePath = function(path){
            $location.path(path);
            if (path === '/' || path === '/customers'){
                this.currentMenu = 'customers';
            } else if (path === '/products'){
                this.currentMenu = 'products';
            } else {
                this.currentMenu = 'none';
            }

        };

    }]);

})();