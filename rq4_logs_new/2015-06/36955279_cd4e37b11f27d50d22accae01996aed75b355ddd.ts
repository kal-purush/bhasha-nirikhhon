/**
 * Created by BryFa on 05/06/15.
 */
/// <reference path="_References.ts" />
module Application {
    'use strict';
    export class AppConfig {
        constructor($stateProvider:ng.ui.IStateProvider, $urlRouterProvider:ng.ui.IUrlRouterProvider) {
            $urlRouterProvider.otherwise('/');
            $stateProvider
                .state("module1", <ng.ui.IState>
                {
                    abstract: false,
                    url: '/module1',
                    templateUrl: 'app/modules/module1/main.html',
                   // controller: "AppController"
                })
                .state("home", <ng.ui.IState>
                {
                    abstract: false,
                    url: '/',
                    templateUrl: 'app/views/home.html',
                    //controller: "AppController"
                })
                .state("about", <ng.ui.IState>
                {
                    abstract: false,
                    url: '/about',
                    templateUrl: 'app/views/about.html',
                    //controller: "AppController"
                });
        }
    }
}

angular.module('app', ['ui.router'])
    .config(["$stateProvider", "$urlRouterProvider",
        ($stateProvider, $urlRouterProvider) => {
            return new Application.AppConfig($stateProvider, $urlRouterProvider);
        }
    ]);