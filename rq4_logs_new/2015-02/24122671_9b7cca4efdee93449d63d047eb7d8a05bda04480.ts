angular.module('TruckMuncherApp').directive('circularImage', ['$timeout', function ($timeout: ng.ITimeoutService) {
    var desiredSize = 50;

    function getOffset(biggerVal:number, smallerVal:number) {
        var newWidth = desiredSize * biggerVal / smallerVal;
        if (newWidth > desiredSize) {
            return "-" + Math.floor((newWidth - desiredSize) / 2) + "px";
        } else {
            return "0";
        }
    }

    return {
        restrict: 'A',
        link: function (scope:ng.IScope, elem:ng.IRootElementService, attrs:ng.IAttributes) {
            elem.on('load', function () {
                $timeout(function () {
                    var width = elem.width();
                    var height = elem.height();
                    if (width < height) {
                        attrs.$addClass('circular-image-tall');
                        var offset = getOffset(height, width);
                        attrs.$set('style', "margin-top:" + offset);
                    } else {
                        attrs.$addClass('circular-image-wide');
                        var offset = getOffset(width, height);
                        attrs.$set('style', "margin-left:" + offset);
                    }
                })
            })
        }
    }
}]);