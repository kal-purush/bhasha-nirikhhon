import {
  TestComponentBuilder,
  describe,
  expect,
  injectAsync,
  it
} from 'angular2/testing';

import {Component, View} from 'angular2/angular2';

import {ROUTER_DIRECTIVES} from 'angular2/router';

import {DOM} from 'angular2/src/core/dom/dom_adapter';

import {InfoCmp} from './info';



export function main() {
  describe('Info component', () => {
    it('should work', injectAsync([TestComponentBuilder], (tcb: TestComponentBuilder) => {
      return tcb.overrideTemplate(TestComponent, '<div><info></info></div>')
        .createAsync(TestComponent)
        .then((rootTC) => {
          rootTC.detectChanges();

          let aboutInstance = rootTC.debugElement.componentViewChildren[0].componentInstance,
            aboutDOMEl = rootTC.debugElement.componentViewChildren[0].nativeElement;


          console.log(aboutInstance);

          rootTC.detectChanges();


        });
    }));
  });
}

@Component({ bindings: [], selector: 'test-cmp' })
@View({directives: [InfoCmp]})
class TestComponent {}