import * as angular from 'angular';
import 'angular-mocks';
import {cartItem} from './cart-item';

describe('cartItem component', () => {
  beforeEach(() => {
    angular
      .module('cartItem', ['app/components/cart-item/cart-item.html'])
      .component('cartItem', cartItem);
    angular.mock.module('cartItem');
  });

  it('should...', angular.mock.inject(($rootScope: ng.IRootScopeService, $compile: ng.ICompileService) => {
    const element = $compile('<cart-item></cart-item>')($rootScope);
    $rootScope.$digest();
    expect(element).not.toBeNull();
  }));
});