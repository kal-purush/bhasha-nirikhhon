/// <reference path="../../typings/tsd.d.ts" />


module myApp {
    export const appModuleId = 'app';

    'use strict';

    angular.module(appModuleId, [
        shellModuleId,
        homeModuleId,
        dataModuleId,
        'ui.router'
    ])
    .config(Config)
    .run(Run);

    function Config($urlRouterProvider: ng.ui.IUrlRouterProvider, $stateProvider: angular.ui.IStateProvider, $sceProvider: angular.ISCEProvider) {
        $urlRouterProvider.otherwise('/home');
        $sceProvider.enabled(false);
    }
    Config.$inject = ['$urlRouterProvider', '$stateProvider', '$sceProvider'];

    function Run($rootScope: angular.IRootScopeService, $state: angular.ui.IStateService, $stateParams: angular.ui.IStateParamsService) {
        $rootScope["$state"] = $state;
        $rootScope["$stateParams"] = $stateParams;
        $rootScope.$on('$stateChangeSuccess', function (event, toState, toParams, fromState, fromParams) {
            if (angular.isDefined(toState.data)) {
                if (angular.isDefined(toState.data.pageTitle)) {
                    $rootScope["pageTitle"] = toState.data.pageTitle + ' | Troi';
                }
            }
        });
    }
    Run.$inject = ['$rootScope', '$state', '$stateParams']; 
}