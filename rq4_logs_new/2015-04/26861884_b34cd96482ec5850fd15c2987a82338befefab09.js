/**
 * Created by Peter on 25.04.2015.
 */
(function() {
    var app = angular.module('dreams-directives', []);
    app.directive('mercTitle', function() {
        return {
            restrict: 'E',
            templateUrl: 'merc-title.html'
        };
    });

    app.directive('mercSpecs', function() {
        return {
            restrict: 'A',
            templateUrl: 'merc-specs.html'
        };
    });

    app.directive('mercTabs', function() {
        return {
            restrict: 'E',
            templateUrl: 'merc-tabs.html',
            controller: function() {
                this.tab = 1;
                this.setTab = function (newValue) {
                    this.tab = newValue;
                };
                this.isSet = function (tabName) {
                    return this.tab === tabName;
                }
            },
            controllerAs: 'tab'
        };
    });

    app.directive('mercGallery', function() {
        return {
            restrict: 'E',
            templateUrl: 'merc-images.html',
            controller: function() {
                this.current = 0;
                this.setCurrent = function(newValue) {
                    this.current = newValue;
                }
            },
            controllerAs: 'gallery'
        };
    });
})();