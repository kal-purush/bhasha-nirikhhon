import { TestBed, ComponentFixture } from '@angular/core/testing';
import { Component } from '@angular/core';
import { createGenericTestComponent } from '../../test/util/helpers';
import { NglDynamicIconsModule } from './module';

const createTestComponent = (html?: string, detectChanges?: boolean) =>
  createGenericTestComponent(TestComponent, html, detectChanges) as ComponentFixture<TestComponent>;

describe('`ngl-dynamic-icon-eq`', () => {

  beforeEach(() => TestBed.configureTestingModule({
    declarations: [TestComponent],
    imports: [NglDynamicIconsModule],
  }));

  it('should render correctly', () => {
    const fixture = createTestComponent();
    const host = fixture.nativeElement.firstElementChild.firstElementChild;

    expect(host).toHaveCssClass('slds-icon-eq');
  });
});

@Component({ template: '<ngl-dynamic-icon-eq></ngl-dynamic-icon-eq>' })
export class TestComponent {
}