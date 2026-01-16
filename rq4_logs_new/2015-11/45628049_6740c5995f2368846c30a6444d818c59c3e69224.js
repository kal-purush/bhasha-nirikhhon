(function() {
  'use strict';

  /**
   * Specify controller for iot.live module.
   *
   * @namespace Controllers
   */
  angular
    .module('iot.live')
    .controller('DashboardController', DashboardController)
  ;

  /**
   * @ngInject
   *
   * @constructor
   */
  function DashboardController() {
    var vm = this;

    vm.getValue = function getValue(id) {
      return _.find(vm.dashboard, {$id: id});
    };
  }
})();