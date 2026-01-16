
module myApp {
    'use strict';

    angular.module(appModuleId, [
        shellModuleId,
        homeModuleId,
        dataModuleId,
        commonModuleId,
        'ui.router'
    ])
    .config(Config)
    .run(Run);

    function Config($urlRouterProvider: ng.ui.IUrlRouterProvider) {
        $urlRouterProvider.otherwise('/home');
        // greetingProvider.setGreeting("sup?");
    }
    Config.$inject = ['$urlRouterProvider'];

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