module op {
    'use strict';

    /* @ngInject */
    function config($locationProvider: ng.ILocationProvider,
        $urlRouterProvider: ng.ui.IUrlRouterProvider,
        $compileProvider: ng.ICompileProvider,
        $logProvider: ng.ILogProvider,
        RELEASE: boolean): void {

        $locationProvider.html5Mode(true);
        $urlRouterProvider.otherwise('/');
        $compileProvider.debugInfoEnabled(!RELEASE);
        $logProvider.debugEnabled(!RELEASE);
    }

    angular.module('op')
      .config(config);
}