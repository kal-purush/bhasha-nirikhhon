import {TestBed, ComponentFixture}  from '@angular/core/testing';
import {Component} from '@angular/core';
import {createGenericTestComponent} from '../../test/util/helpers';
import {NglAlertModule} from './module';

const createTestComponent = (html?: string, detectChanges?: boolean) =>
  createGenericTestComponent(TestComponent, html, detectChanges) as ComponentFixture<TestComponent>;

describe('`ngl-alert`', () => {

  beforeEach(() => TestBed.configureTestingModule({declarations: [TestComponent], imports: [NglAlertModule]}));

  it('should have the proper classes and attributes', () => {
    const fixture = createTestComponent();
    const hostElement = fixture.nativeElement.querySelector('.slds-notify');

    expect(hostElement).toHaveCssClass('slds-notify_alert');
  });
});


@Component({
  template: `
    <ngl-alert></ngl-alert>
  `,
})
export class TestComponent {
}