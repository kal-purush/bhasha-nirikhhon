import * as angular from 'angular';

class AppCtrl {
  static $inject = ['$log'];
  constructor(private $log: ng.ILogService) {
    $log.debug('app controller instantiated');
  }
}

angular.module('first-ng-ts-app', [])
.controller('app-ctrl', AppCtrl);