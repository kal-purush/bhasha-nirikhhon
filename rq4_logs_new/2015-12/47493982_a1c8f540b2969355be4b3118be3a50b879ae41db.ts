import {
  it,
  iit,
  describe,
  ddescribe,
  expect,
  inject,
  injectAsync,
  TestComponentBuilder,
  beforeEachProviders
} from 'angular2/testing';
import {provide} from 'angular2/core';
import {IOCmp} from './io';


describe('IO Component', () => {

  beforeEachProviders(() => []);


  it('should ...', inject([TestComponentBuilder], (tcb:TestComponentBuilder) => {
    tcb
      .overrideTemplate(IOCmp, 'x')
      .createAsync(IOCmp).then((fixture) => {
        fixture.detectChanges();
      });
  }));

});