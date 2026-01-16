/**
 * AngularJS directive used to check if the email input already exists.
 * Used in soundfloor.net
 *
 * @author jolcaleb
 */

(function(){
  'use strict';

  angular
    .module('emailDuplicateChecker')
    .directive('emailDuplicateChecker', emailDuplicateChecker);


  /** make GET RESTful api which returns email address already exists if api returns boolean data
   * example: <input api="/api/v1/users" query="email" ng-model="vm.form.email"/> will call api '/api/v1/users?email={{ vm.form.email }}'
   * < RESULT according to return value >
   * true: the email address already exists
   * false: the email address does not exist
   */

  /* @ngInject */
  function emailDuplicateChecker ($http, $q) {
    var directive = {
      restrict: 'A',
      require: 'ngModel',

      scope: {
        api: '@',
        query: '@'
      },
      link: linkFunc
    };

    return directive;

    function linkFunc(scope, element, attrs, modelCtrl) {
      if (modelCtrl && modelCtrl.$validators.email) {
        modelCtrl.$asyncValidators.isDuplicate = function (modelValue, viewValue) {
          if (modelCtrl.$isEmpty(modelValue)) {
            return $q.when();
          }

          var deferred = $q.defer();

          $http.get(scope.api + '?' + scope.query + '=' + modelValue).then(function successCallback(result) {
            if (result.data === true) {
              deferred.reject(); // will make form.$error.isDuplicate true
            }
            else {
              deferred.resolve(); // will make form.$error.isDuplicate undefined.
            }
          }, function failureCallback(result) {
            console.log(result);
          });

          return deferred.promise;
        };
      }
    }
  }
})();