var app = angular.module('uiRouterExtrasExample', [
    'ui.router',
    'ct.ui.router.extras.core',
    'ct.ui.router.extras.sticky',
    'ct.ui.router.extras.dsr',
    'ct.ui.router.extras.previous',
    'ct.ui.router.extras.statevis',

    'uiRouterExtrasExample.tabs']);

app.config(['$stateProvider', '$stickyStateProvider',
    ($stateProvider: ng.ui.IStateProvider, $stickyStateProvider: ng.ui.extras.IStickyStateProvider) => {

   
}]);

app.run(['$rootScope', '$state', ($rootScope: any, $state: ng.ui.IStateService) => {
    $rootScope.$state = $state;
}]);
