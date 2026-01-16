'use strict';

angular.module('dailyRouteApp')
    .controller('SettingsCtrl', [
        '$scope',
        'RouteDataService',
        function ($scope, RouteDataService) {
            $scope.clearData = function () {
                RouteDataService.clear();
            };
        }
    ]);