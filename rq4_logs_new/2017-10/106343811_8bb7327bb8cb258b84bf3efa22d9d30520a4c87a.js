'use strict';

/**
 * @ngdoc function
 * @name frontApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the frontApp
 */
angular.module('frontApp')
    .controller('headercontroller', ['contenidoFactory', '$scope', '$window', '$http', 'API_PATH', 'API_PATH_MEDIA', function (contenidoFactory, $scope, $window, $http, API_PATH, API_PATH_MEDIA) {

        $scope.API_PATH_MEDIA = API_PATH_MEDIA;

        contenidoFactory.ServiceContenido('manager/ImagenHeader/', 'GET', {}).then(function (data) {
            console.log(data.data);
            $scope.header = data.data[0];
        });

        contenidoFactory.ServiceContenido('manager/Menu/', 'GET', {}).then(function (data) {
            console.log(data.data);
            $scope.menu = data.data[0];
        });
    }]);