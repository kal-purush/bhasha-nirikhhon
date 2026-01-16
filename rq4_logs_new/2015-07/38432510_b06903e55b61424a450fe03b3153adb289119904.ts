module toolbar {

    class ToolbarDirective implements ng.IDirective {

        static instance(sidenav): ng.IDirective {
            return new ToolbarDirective(sidenav);
        }
        constructor(private sidenav) { }

        restrict = 'E';
        templateUrl = 'templates/toolbar.tpl.html';
        replace = true;
        transclude = true;
        controller = ($scope) => {
            $scope.toggleSidenav = () => {
                this.sidenav.toggle();
            }
        }
    }

    angular.module('toolbar.directive', ['sidenav'])
        .directive('i2rToolbar', ['sidenav.service.action',ToolbarDirective.instance]);
}