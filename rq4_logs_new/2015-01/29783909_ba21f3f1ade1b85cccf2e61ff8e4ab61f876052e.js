'use strict';

angular.module('Photoweather').
directive('currently', function(auxServices, tempObject) {
    return {
        restrict: 'A',
        link: function(scope, element) {
            element[0].width = window.innerWidth;
            element[0].height = window.innerHeight;
            var ctx = element[0].getContext('2d');

            scope.$watch("forecast", function(newVal, oldVal) {
                if (newVal) {
                    // Color temperature full screen box
                    ctx.fillStyle = "rgb(" + tempObject[auxServices.roundTo5th(scope.forecast.currently.temperature)] + ")";
                    ctx.fillRect(0, 0, element[0].clientWidth, element[0].clientHeight);

                    // "Currently"
                    ctx.font = "21px league-gothic";
                    ctx.fillStyle = "rgba(255,255,255,0.5)";
                    ctx.fillText("CURRENTLY", 25, element[0].clientHeight - 130);

                    // Numerical temperature
                    ctx.font = "64px lato";
                    ctx.fillStyle = "rgba(255,255,255,1)"
                    ctx.fillText(auxServices.roundToHalf(scope.forecast.currently.temperature) + "º", 25, element[0].clientHeight - 75);
                }
            });
        }
    }
})