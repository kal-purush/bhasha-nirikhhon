/**
 * Created by AaronYuan on 9/20/15.
 */

/// <reference path="../../app.ts" />
module WeMedia {
    'use strict';

    export interface IBindCardScope extends IWMBaseScope {
        tabIndex: number;

    }


    class BindCard {
        constructor(
            public $rootScope: IWMRootScope,
            public $scope: IBindCardScope
        ){
            $scope.tabIndex = 0;


        }

    }
    BindCard.$inject = ['$rootScope','$scope'];
    ControllerModule.controller('BindCardCtrl', BindCard);

}