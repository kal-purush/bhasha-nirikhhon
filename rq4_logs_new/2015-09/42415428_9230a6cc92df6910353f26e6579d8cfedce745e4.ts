/// <reference path="../../typings/tsd.d.ts" />

module Generator {
    var app = angular.module("incremental", []);

    export interface GeneratorScope extends ng.IScope {
        value: number;

        generate():void;
    }

    export class GeneratorController {
        static $inject = ['$scope'];

        constructor ($scope: GeneratorScope) {
            $scope.value = 0;
            $scope.generate = () => {
                $scope.value++;
            }
        }
    }

    app.controller("GeneratorController", GeneratorController);
}