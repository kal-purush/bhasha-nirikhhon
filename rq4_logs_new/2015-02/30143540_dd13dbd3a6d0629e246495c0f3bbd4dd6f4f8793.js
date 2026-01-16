'use strict';


angular.module('storageApp')
    .controller('DataReplicationRunningCtrl', ['$scope', '$routeParams', '$location', 'diskService',
        function ($scope, $routeParams, $location, diskService) {
            $scope.currentPage = 1;
            $scope.pageSize = 10;
            $scope.status;
            $scope.disks = [];
            $scope.loading = true;
            getdisks();

            function getdisks() {
                diskService.listdisks()
                    .success(function (data) {
                        $scope.loading = false;
                        $scope.disks = data;
                    })
                    .error(function (error) {
                        $scope.status = 'Unable to load disk data: ' + error.message;
                        $scope.loading = false;
                    });
            }
}]);