'use strict';

class WerktDit {
  constructor (private $http,
                 private _) {}
}

angular
    .module('app')
    .service('WerktDit', ['$http', WerktDit])