import {TestBed, ComponentFixture}  from '@angular/core/testing';
import {Component} from '@angular/core';
import {createGenericTestComponent} from '../../test/util/helpers';
import {NglIconsModule} from './module';

const createTestComponent = (html?: string, detectChanges?: boolean) =>
  createGenericTestComponent(TestComponent, html, detectChanges) as ComponentFixture<TestComponent>;

describe('`ngl-dynamic-icon`', () => {

  beforeEach(() => TestBed.configureTestingModule({
    declarations: [TestComponent],
    imports: [NglIconsModule],
  }));

  it('should render correctly the waffle icon', () => {
    const fixture = createTestComponent();
    const host = fixture.nativeElement.querySelector('span');

    expect(host.firstElementChild.tagName.toLowerCase()).toBe('ngl-dynamic-icon-waffle');
  });

  it('should propagate the click event of waffle icon', () => {
    const fixture = createTestComponent();
    const button = fixture.nativeElement.querySelector('button');

    button.click();
    fixture.detectChanges();

    expect(fixture.componentInstance.onClick).toHaveBeenCalled();
  });
});

@Component({ template: '<span ngl-dynamic-icon nglDynamicIconType="waffle" (onClick)="onClick($event)"></span>' })
export class TestComponent {

  onClick = jasmine.createSpy();
}