/// <reference path="../references/angularjs/angular.d.ts" />
/// <reference path="service.ts" />

class DemoController {
    static $inject:string[] = ['$scope', 'modelFactory'];
    model:Model;

    constructor($scope:ng.IScope, modelFactory) {
        this.model = modelFactory.create();

        $scope.$watch(() => {
            return this.model;
        }, () => {
            this.logLol()
        }, true);
    }

    public logLol() {
        console.log('lol');
    }
}

var app:ng.IModule = angular.module('demoApp');
app.controller('demoController', DemoController);