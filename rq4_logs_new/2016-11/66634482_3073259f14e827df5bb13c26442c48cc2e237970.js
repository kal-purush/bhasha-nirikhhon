avBait.directive("scroll", function ($window) {
    return {
        link: function (scope, element, attrs) {
            angular.element($window).bind("scroll", function () {
                if (this.pageYOffset >= attrs.limit * 1) {
                    scope.changeClass = true;
                } else {
                    scope.changeClass = false;
                }
                scope.$apply();
            });
        }
    }
});