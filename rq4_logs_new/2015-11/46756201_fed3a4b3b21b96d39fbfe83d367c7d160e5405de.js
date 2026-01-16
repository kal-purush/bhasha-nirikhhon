(function() {

  'use strict';

  angular.module('experiments.loading.directive', [])

  .directive('loading', [function() {

    return {
      restrict: 'E',
      templateUrl: 'components/loading/loading.html',
      link: function(scope, elem, attrs) {

        scope.$watch('loading', function(newValue, oldValue) {
          if (newValue) {
            $('#loading-mask').show();
            $('#loading-image').show();
          } else {
            $('#loading-mask').hide();
            $('#loading-image').hide();
          }

        });

      }

    }

  }]);

})();