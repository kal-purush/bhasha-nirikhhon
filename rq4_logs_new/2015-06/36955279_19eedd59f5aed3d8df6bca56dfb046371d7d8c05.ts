/**
 * Created by BryFa on 09/06/15.
 */
/// <reference path="../../_References.ts" />
module Application.Module2 {
    export class ModuleConfig {
        constructor($stateProvider:ng.ui.IStateProvider, $urlRouterProvider:ng.ui.IUrlRouterProvider) {
            $stateProvider
                .state("module2", <ng.ui.IState>
            {
                abstract: false,
                url: '/module2',
                templateUrl: 'app/modules/module2/main.html',
                controller: Application.Module2.ModuleController
            });
        }
    }
}

angular.module('app.module2', ['ui.router'])
    .config(["$stateProvider", "$urlRouterProvider",
        ($stateProvider, $urlRouterProvider) => {
            return new Application.Module2.ModuleConfig($stateProvider, $urlRouterProvider);
        }
    ]);