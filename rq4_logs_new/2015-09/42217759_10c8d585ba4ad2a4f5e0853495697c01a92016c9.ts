/**
 * Created by AaronYuan on 15/9/13.
 */
/// <reference path="../../app.ts" />
/// <reference path="../../service/OrderService.ts" />

module WeMedia {
    'use strict';

    declare var window;

    class PrecontactList {
        constructor(
            public $rootScope: IWMRootScope,
            public $scope: any,
            public OrderService: any
        ){



            this.init();
        }

        init() {
            this.$scope.dataList = [1];
            this.$scope.currentPageIndex = 1;
            this.$scope.totalItems = 0;
            this.$scope.pageSize = 15;

            this.refresh();
        }


        refresh(args = {}){
            var self = this;
            args =  angular.extend({}, args);
            this.OrderService.myOrderList(args).then(function(result){
                if(result && result.Data){
                    self.$scope.dataList = result.Data;
                    self.$scope.totalItems = result.TotalItems;
                }
            }, function(){
            });


        }



    }

    PrecontactList.$inject = ['$rootScope', '$scope', 'OrderService'];
    ControllerModule.controller('PrecontactListCtrl', PrecontactList);

}