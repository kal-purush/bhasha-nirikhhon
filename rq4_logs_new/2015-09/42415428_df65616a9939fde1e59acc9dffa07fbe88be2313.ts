/// <reference path="../../../typings/tsd.d.ts" />

module Generator {
    var app = angular
        .module("incremental", [])
        .directive('generator', () => new GeneratorDirective())
        .controller("GeneratorController", GeneratorController);

    export class GeneratorDirective implements ng.IDirective {
        public templateUrl: string;
        public restrict: string;
        public scope: any;
        public controller: any;
        public controllerAs: string;
        public bindToController: boolean;

        constructor () {
            this.templateUrl = 'directives/generator/generator.html';
            this.restrict = 'E';
            this.scope = {
                value: "="
            };
            this.controller = GeneratorController;
            this.controllerAs = "vm";
            this.bindToController = true;
        }
    }

    export class GeneratorController {
        public value: number;

        public generate() {
            this.value++;
        }
    }
}