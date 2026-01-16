import HomeController = require("./controllers/HomeController");
import UnitsService = require("./services/UnitsService");
import UserService = require("./services/UserService");
import Angular = require("./dependencies/Angular");
import AngularResource = require("./dependencies/Angular-resource");
import AngularRoute = require("./dependencies/Angular-route");
import Kendo = require("./dependencies/Kendo");

export module app {
    Kendo;
    AngularRoute;
    AngularResource;

    var main = Angular.module('scadaModule', ['kendo.directives', 'ngRoute', 'ngResource']);
    main.controller('homeController', HomeController);
    main.service("unitsService", UnitsService);
    main.service("usersService", UserService);
    main.config(routeConfig);

    routeConfig.$inject = ["$routeProvider"];
    function routeConfig($routeProvider: ng.route.IRouteProvider): void {
        $routeProvider
            .when('/home', {
                templateUrl: 'templates/home.html',
                controller: 'homeController as home'
            }).otherwise('/home');
    }
}