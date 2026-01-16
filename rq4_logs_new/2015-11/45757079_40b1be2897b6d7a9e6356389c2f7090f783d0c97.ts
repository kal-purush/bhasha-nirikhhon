/// <reference path="../../app.d.ts" />

'use strict';

(function () {

  // @ngInject
  function braintreeService ($http: ng.IHttpService): app.IBraintreeService {

		var api = '/api/braintree';

    return {
      productCheckout: (data, productId) => {
        return $http.post(`${api}/${productId}`, data);
      },
      getClientToken: () => {
        return $http.get(api);
      }
    };
  }

  angular.module('backend.braintree', [])
    .factory('BraintreeService', braintreeService);

})();