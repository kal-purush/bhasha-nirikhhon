'use strict';

angular.module('polaroiz').directive('loopButtons', function () {

    return {
        restrict: 'E',
        templateUrl: 'app/views/loop-buttons.html',
        controller: function ($scope) {

            var pictureElements = [];
            var index = 0;

            $scope.$on('images_loaded', function () {
                pictureElements = $('.polaroid-outer');
            });

            $scope.next = function () {
                setTimeout(function () {
                    pictureElements[index].click();
                }, 1);
                if (index < pictureElements.length) {
                    index++;
                } else {
                    index = 0;
                }
            };

            $scope.previous = function () {
                if (index > 0) {
                    index--;
                } else {
                    index = pictureElements.length;
                }
                setTimeout(function () {
                    pictureElements[index].click();
                }, 1);
            };
        }

    };

});