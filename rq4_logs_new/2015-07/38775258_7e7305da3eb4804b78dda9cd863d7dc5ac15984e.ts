///<reference path="../../tools/typings/tsd.d.ts" />
((): void => {

    var app = angular.module('demoApp', ['ngRoute', 'ngAnimate',"templates"]);

    app.config(['$routeProvider', ($routeProvider) => {
        $routeProvider.when('/',
        {
            controller: 'demoApp.CustomersController',
            templateUrl: 'customers.html',
            controllerAs: 'vm'
        })
        .when('/orders/:customerId',
        {
            controller: 'demoApp.OrdersController',
            templateUrl: 'orders.html',
            controllerAs: 'vm'
        });
    }]);

})();