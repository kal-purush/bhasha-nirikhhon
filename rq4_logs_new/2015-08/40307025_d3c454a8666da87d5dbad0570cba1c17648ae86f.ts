/// <reference path="../../index.module.ts" />

module shop {
  'use strict';

  interface IProductScope extends ng.IScope {
    product: IProduct;
  }

  /** @ngInject */
  export function productDetails(): ng.IDirective {

    return {
      restrict: 'E',
      scope: {
        product: '='
      },
      templateUrl: 'app/components/product/product.details.html',
      controller: ProductDirectiveDetailsController,
      controllerAs: 'pctddtct',
      link: linkFunc,
      //bindToController: true
    };

    function linkFunc(scope: IProductScope, el: JQuery, attr: any, pctddtct: ProductDirectiveDetailsController) {

    }

  }

  /** @ngInject */
  class ProductDirectiveDetailsController {
    private product: IProduct

    constructor(moment: moment.MomentStatic, $location: ng.ILocationService) {

    }
  }
}