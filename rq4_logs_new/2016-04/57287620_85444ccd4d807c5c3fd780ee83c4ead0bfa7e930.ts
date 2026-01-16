import {
    ComponentFixture,
    TestComponentBuilder,
    beforeEach,
    beforeEachProviders,
    ddescribe,
    xdescribe,
    describe,
    expect,
    iit,
    inject,
    it,
    xit,
} from 'angular2/testing';
import {Component} from 'angular2/core';
import {FooDirective} from './foo.directive';
import {DOM} from "angular2/src/platform/dom/dom_adapter";


describe(FooDirective, () => {
    beforeEachProviders(() => [
        TestComponentBuilder
    ]);

    it('test', inject([TestComponentBuilder], (tcb: TestComponentBuilder) => {
        const template = '<div [foo]="value"></div>';

        return tcb.overrideTemplate(TestComponent, template).createAsync(TestComponent).then((fixture: ComponentFixture) => {
            let div = DOM.firstChild(fixture.nativeElement);
            let component = <TestComponent>fixture.componentInstance;
            const className = 'aaa';
            expect(DOM.hasClass(div, className)).toBeFalsy();
            component.value = className;
            fixture.detectChanges();
            expect(DOM.hasClass(div, className)).toBeTruthy();
        });
    }));
});

@Component({
    selector: 'test-cmp',
    directives: [FooDirective],
    template: ''
})
class TestComponent {
    value = '';

    constructor() {}
}