'use strict';

/**
 * @ngdoc directive
 * @name ngblogApp.directive:ueditorDirective
 * @description
 * # ueditorDirective
 */
angular.module('ngblogApp')
  .directive('ueditorDirective', function () {
    return {
      template: '<div></div>',
      restrict: 'E',
      link: function postLink(scope, element, attrs) {
        element.text('this is the ueditorDirective directive');
      }
    };
  });