 'use strict';

module.exports = function(app) {

  app.directive('clickedDir', function() {
    return {
      restrict: 'E',
      replace: true,
      templateUrl: '/html/clickedDir.html',
      transclude: true
    };
  });

};