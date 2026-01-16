'use strict';

module AppComponent {
  console.log('app component');
  
  /* @ngInject */
  function AppDirective () {
    return {
      restrict: 'E',
      templateUrl: 'app/app.html',
      controller: 'AppController'
    }
  }
  
  /* @ngInject */
  function AppController($router) {
    $router.config([
      {
        path: '/product',
        component: 'product',
        as: 'Product'
      }
    ]);
  }
	
	angular.module('braintreeClient')
    .directive('app', AppDirective)
    .controller('AppController', AppController);
  
}

export {AppComponent as default};