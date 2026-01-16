"use strict";

module Affecto.Base
{
    export interface IController
    {
    }

    export class Controller implements IController
    {
        constructor($scope: angular.IScope)
        {
            $scope.$on("$destroy", (event: angular.IAngularEvent, ...args: any[]): void =>
            {
                this.dispose();
            });
        }

        protected dispose()
        {
        }
    }
}