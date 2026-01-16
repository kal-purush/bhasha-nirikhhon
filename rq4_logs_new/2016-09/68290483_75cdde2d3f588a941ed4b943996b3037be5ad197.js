(function() {
  'use strict';

  angular
    .module('test')
    .controller('DialogController', DialogController);

  /** @ngInject */
  function DialogController($timeout, webDevTec, toastr, $state, $scope, obj) {
    var vm = this;
    $scope.article = obj;

    $scope.hide = false;

    $scope.click = function() {
      $scope.hide = true;
    }
  }
})();