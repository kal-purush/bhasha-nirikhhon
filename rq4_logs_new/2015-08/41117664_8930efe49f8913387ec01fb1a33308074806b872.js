'use strict';

describe('Directive: threecardpokerhand', function () {

  // load the directive's module and view
  beforeEach(module('pkerApp'));
  beforeEach(module('app/directives/threecardpokerhand/threecardpokerhand.html'));

  var element, scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

  it('should make hidden element visible', inject(function ($compile) {
    element = angular.element('<threecardpokerhand></threecardpokerhand>');
    element = $compile(element)(scope);
    scope.$apply();
    expect(element.text()).toBe('this is the threecardpokerhand directive');
  }));
});