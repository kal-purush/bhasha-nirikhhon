define([
    'angular',
    'components/timeline/module',
    'components/timeline/TimelineController',
    'components/timeline/TimelineServices'
], function (ng, module) {
    'use strict';

    module.directive('uiTimelineComponent', ['TimelinePromise', function (TimelinePromise) {

        return {
            restrict: 'A',
            controller: 'TimelineController',
            templateUrl: 'timeline-template.html',

            link: function (scope, element, attrs) {
                TimelinePromise.getDefer().resolve();
            }
        }
    }]);

    return module;
});
