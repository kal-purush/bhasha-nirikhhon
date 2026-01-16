/**
 * Created by AaronYuan on 9/14/15.
 */
/// <reference path="../../app.ts" />
/// <reference path="../../service/BillService.ts" />
module WeMedia {
    'use strict';

    declare  var window, ZENG;

    class Bill {
        constructor(
            public $rootScope: IWMRootScope,
            public $scope: any,
            public BillService: IBilService
        ){
            $scope.payStatus = angular.bind(this, this.payStatus);
            $scope.searchHandler = angular.bind(this,this.searchHandler);

            console.log($rootScope.user);
            this.init();
            this.refresh();
        }

        init() {
            this.$scope.payRecords = [];
            this.$scope.currentPageIndex = 0;
            this.$scope.totalItems = 0;
            this.$scope.pageSize = 20;

            this.$scope.search = {
                startTime: '',
                endTime:''
            };




        }
        refresh(args=null) {
            var self = this;
            self.BillService.list(args).then(function(result){
                if(result && result.Data){
                    self.$scope.payRecords = result.Data;
                    self.$scope.totalItems = result.TotalItems;
                }
            }, function(err){
                self.$scope.payRecords = [];
                self.$scope.totalItems = 0;
            });
        }

        pageChanged(page) {
            this.$scope.currentPageIndex = page;
            this.refresh({
                page: page,
                pageSize: this.$scope.pageSize
            });
        }

        payStatus(code) {
            switch (code *1){
                case 1:
                    return '待审核';
                case 2:
                    return '失败';
                case 3:
                    return '成功';
            }

        }

        searchHandler(): void{
            var d1 = new Date(this.$scope.search.startTime),
                d2 = new Date(this.$scope.search.endTime);

            if(d1 =='Invalid Date'){
                ZENG.msgbox.show('请选择开始时间',5);
                return null;
            } else if(d2 == 'Invalid Date'){
                ZENG.msgbox.show('请选择结束时间',5);
                return null;
            }
            this.refresh({
                page: this.$scope.currentPageIndex,
                pageSize: this.$scope.pageSize,
                startTime: d1,
                endTime: d2
            });

        }

    }

    Bill.$inject = ['$rootScope', '$scope', 'BillService'];
    ControllerModule.controller('advertiserBllCtrl', Bill);

}

