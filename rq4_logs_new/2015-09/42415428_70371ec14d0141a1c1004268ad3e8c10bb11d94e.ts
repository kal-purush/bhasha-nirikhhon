/// <reference path="../../../typings/tsd.d.ts" />

module Converter {
    var app = angular
        .module("incremental.converter", [])
        .directive('converter', () => new ConverterDirective())
        .controller("ConverterController", ConverterController);

    export class ConverterDirective implements ng.IDirective {
        public templateUrl: string;
        public restrict: string;
        public scope: any;
        public controller: any;
        public controllerAs: string;
        public bindToController: boolean;

        constructor () {
            this.templateUrl = 'directives/converter/converter.html';
            this.restrict = 'E';
            this.scope = {
                text: "=",
                source: "=",
                dest: "="
            };
            this.controller = ConverterController;
            this.controllerAs = "vm";
            this.bindToController = true;
        }
    }

    export class ConverterController {
        public text: string;
        public source: number;
        public dest: number;

        public convert() {
            if (this.source > 0) {
                this.source--;
                this.dest++;
            }
        }
    }
}