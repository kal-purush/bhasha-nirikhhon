"use strict";

module Affecto.BusyIndication
{
    export var BusyIndicatorFactory: angular.IDirectiveFactory = (): BusyIndicator =>
    {
        return new BusyIndicator();
    }

    export class BusyIndicator extends Base.Directive
    {
        constructor()
        {
            super(Base.DirectiveRestriction.Attribute | Base.DirectiveRestriction.Element, "App/Packages/BusyIndication/Directives/BusyIndicator.html");
        }

        protected linkDirective($scope: IBusyIndicatorScope, element: JQuery, attributes?: angular.IAttributes, controller?: any): any
        {
            $scope.$on(Events.toggleBusyIndicator,(event: any, show: boolean, text: string) =>
            {
                $scope.isBusy = show;
                $scope.busyIndicatorText = text;
            });

            var enabled: string = attributes["enabled"];
            if (enabled != undefined)
            {
                $scope.isBusy = enabled === "" || $scope.$eval(enabled);
            }
            if (attributes["text"] != null)
            {
                $scope.busyIndicatorText = attributes["text"];
            }
        }
    }
}