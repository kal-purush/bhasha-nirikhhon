///<reference path="typings/angularjs/angular.d.ts"/>

module Directives
{
    export var directives : ng.IModule = angular.module("directives", []);

    directives.directive('ngRightClick', ['$parse', ($parse : ng.IParseService) =>
    {
        return (scope, element, attrs) =>
        {
            var fn = $parse(attrs.ngRightClick);
            element.bind('contextmenu', function (event)
            {
                scope.$apply(function ()
                {
                    event.preventDefault();
                    fn(scope, {$event: event});
                });
            });
        };
    }]);

}