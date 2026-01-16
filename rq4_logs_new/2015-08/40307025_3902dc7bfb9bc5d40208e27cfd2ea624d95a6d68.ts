/// <reference path="../../index.module.ts" />

module shop {
  'use strict';

  IComment


  /** @ngInject */
  export function rating(): ng.IDirective {

    return {
      restrict: 'E',
      scope: {
        product: '='
      },
      templateUrl: 'app/components/rating/rating.html',
      controller: RatingController,
      controllerAs: 'rating',
      bindToController: true
    };

  }

  /** @ngInject */
  class RatingController {
    private locationService: ng.ILocationService;
    private UserLoginService: UserLoginService
    private loginEventUnHandler: Function

    constructor($rootScope: ng.IRootScopeService, UserLoginService: UserLoginService) {
      this.UserLoginService = UserLoginService

    }

  }
}