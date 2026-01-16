/// <reference path="./Sidenav.controller.ts"/>

module sidenav {

    class SidenavDirective implements ng.IDirective {

        static instance(): ng.IDirective {
            return new SidenavDirective();
        }
        constructor() { }

        restrict = 'E';
        templateUrl = 'templates/sidenav.tpl.html';
        controller = SidenavController;
        controllerAs = 'sl';
        bindToController = true;
        replace = true;
    }

    angular.module('sidenav.directive', [])
        .directive('i2rSidenav', [SidenavDirective.instance]);
}