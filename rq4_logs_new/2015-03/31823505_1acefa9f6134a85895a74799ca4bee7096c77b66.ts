'use strict';

// Create and register modules
var modules = ['app.controllers', 'app.directives', 'app.filters', 'app.services'];
modules.forEach((module) => angular.module(module, []));

modules.push('ngRoute');
modules.push('firebase');

angular.module('app', modules);

angular.module('app').config(['$routeProvider', function routes ($routeProvider: ng.route.IRouteProvider) {
    $routeProvider
        .when('/', {
            template: '<h1>Home</h1>'
        })
        .when('/login', {
            controller: 'app.controllers.LoginCtrl',
            controllerAs: 'vm',
            templateUrl: 'login.html'
        })
        .when('/messages', {
            controller: 'app.controllers.MessagesCtrl',
            controllerAs: 'vm',
            templateUrl: 'messages.html',
            resolve: {
                'currentAuth': ['Auth', function (auth: AngularFireAuth) {
                    return auth.$waitForAuth();
                }]
            }
        })
        .otherwise('/');
}]);

angular.module('app').run(['$rootScope', '$location', function ($rootScope : ng.IRootScopeService, $location : ng.ILocationService) {
    $rootScope.$on('$routeChangeError', function (event: ng.IAngularEvent, next, previous, error: string) {
        $location.path('/login');
    });
}]);

module app {
    export module controllers { null; }
    export module directives { null; }
    export module filters { null; }
    export module services { null; }

    export interface IController {}
    export interface IDirective {
        restrict: string;
        link($scope: ng.IScope, element: JQuery, attrs: ng.IAttributes): any;
    }
    export interface IFilter {
        filter (input: any, ...args: any[]): any;
    }
    export interface IService {}

    /**
     * Register new controller.
     *
     * @param className
     * @param services
     */
    export function registerController (className: string, services = []) {
        var controller = 'app.controllers.' + className;
        services.push(app.controllers[className]);
        angular.module('app.controllers').controller(controller, services);
    }

    /**
     * Register new filter.
     *
     * @param className
     * @param services
     */
    export function registerFilter (className: string, services = []) {
        var filter = className.toLowerCase();
        services.push(() => (new app.filters[className]()).filter);
        angular.module('app.filters').filter(filter, services);
    }

    /**
     * Register new directive.
     *
     * @param className
     * @param services
     */
    export function registerDirective (className: string, services = []) {
        var directive = className[0].toLowerCase() + className.slice(1);
        services.push(() => new app.directives[className]());
        angular.module('app.directives').directive(directive, services);
    }

    /**
     * Register new service.
     *
     * @param className
     * @param services
     */
    export function registerService (className: string, services = []) {
        var service = className[0].toLowerCase() + className.slice(1);
        services.push(() => new app.services[className]());
        angular.module('app.services').factory(service, services);
    }

}