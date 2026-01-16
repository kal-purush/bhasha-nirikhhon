import { TestBed, ComponentFixture } from '@angular/core/testing';
import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { createGenericTestComponent, dispatchEvent } from '../../../test/util';
import { NglSliderModule } from './module';

const createTestComponent = (html?: string) =>
  createGenericTestComponent(TestComponent, html) as ComponentFixture<TestComponent>;

function getSliderElement(element: Element): HTMLDivElement {
  return <HTMLDivElement>element.firstElementChild;
}

function getSliderInput(element: Element): HTMLInputElement {
  return element.querySelector('input.slds-slider__range');
}

function getSliderValue(element: Element): string {
  return element.querySelector('span.slds-slider__value').textContent;
}

function getSliderLabelRange(element: Element): string {
  return element.querySelector('span.slds-slider-label__range').textContent;
}

describe('Slider Component', () => {

  beforeEach(() => TestBed.configureTestingModule({declarations: [TestComponent], imports: [NglSliderModule, FormsModule]}));

  it('should render correctly default values', () => {
    const fixture = createTestComponent();

    const slider = getSliderElement(fixture.nativeElement);
    expect(slider).toHaveCssClass('slds-form-element');
    expect(getSliderLabelRange(slider)).toEqual('0 - 100');

    const input = getSliderInput(slider);
    expect(input.value).toEqual('0');
    expect(input.getAttribute('min')).toEqual('0');
    expect(input.getAttribute('max')).toEqual('100');
    expect(input.getAttribute('step')).toEqual('1');
    expect(getSliderValue(slider)).toEqual('0');
  });

  it('should display correct label', () => {
    const fixture = createTestComponent(`<ngl-slider [value]="value" [label]="label"></ngl-slider>`);
    const { nativeElement, componentInstance } = fixture;

    const labelEl = nativeElement.querySelector('label.slds-form-element__label');
    expect(labelEl.getAttribute('for')).toEqual(getSliderInput(nativeElement).id);
    expect(labelEl.querySelector('span.slds-slider-label__label')).toBeFalsy();

    componentInstance.label = 'Slider label';
    fixture.detectChanges();
    expect(labelEl.querySelector('span.slds-slider-label__label').textContent).toEqual('Slider label');
  });

  it('should update `value` based on input', () => {
    const fixture = createTestComponent();
    const { nativeElement, componentInstance } = fixture;
    const input = getSliderInput(nativeElement);

    componentInstance.value = 50;
    fixture.detectChanges();
    expect(input.value).toEqual('50');
    expect(getSliderValue(nativeElement)).toEqual('50');

    componentInstance.value = 100;
    fixture.detectChanges();
    expect(input.value).toEqual('100');
    expect(getSliderValue(nativeElement)).toEqual('100');
  });

  it('should keep `value` between mix/max', () => {
    const fixture = createTestComponent();
    const { nativeElement, componentInstance } = fixture;
    const input = getSliderInput(nativeElement);

    componentInstance.value = 110;
    fixture.detectChanges();
    expect(input.value).toEqual('100');
    expect(getSliderValue(nativeElement)).toEqual('100');

    componentInstance.value = -10;
    fixture.detectChanges();
    expect(input.value).toEqual('0');
    expect(getSliderValue(nativeElement)).toEqual('0');
  });

  it('should emit new value when range input changes', () => {
    const fixture = createTestComponent();
    const { nativeElement, componentInstance } = fixture;
    const input = getSliderInput(nativeElement);

    input.value = '75';
    dispatchEvent(input, 'input');
    expect(componentInstance.change).toHaveBeenCalledWith(75);
  });

  it('should configure different min, max and step', () => {
    const fixture = createTestComponent(`<ngl-slider [value]="value" [min]="100" [max]="200" step="20"></ngl-slider>`);
    const { nativeElement } = fixture;

    expect(getSliderLabelRange(nativeElement)).toEqual('100 - 200');

    const input = getSliderInput(nativeElement);
    expect(input.getAttribute('min')).toEqual('100');
    expect(input.getAttribute('max')).toEqual('200');
    expect(input.getAttribute('step')).toEqual('20');
  });

  it('should display different sizes', () => {
    const fixture = createTestComponent(`<ngl-slider [value]="value" [size]="size"></ngl-slider>`);
    const { nativeElement, componentInstance } = fixture;
    const sliderEl = nativeElement.querySelector('div.slds-slider');

    componentInstance.size = 'large';
    fixture.detectChanges();
    expect(sliderEl).toHaveCssClass('slds-size_large');

    componentInstance.size = 'small';
    fixture.detectChanges();
    expect(sliderEl).not.toHaveCssClass('slds-size_large');
    expect(sliderEl).toHaveCssClass('slds-size_small');
  });

  it('should display vertically', () => {
    const fixture = createTestComponent(`<ngl-slider [value]="value" vertical></ngl-slider>`);
    const sliderEl = fixture.nativeElement.querySelector('div.slds-slider');
    expect(sliderEl).toHaveCssClass('slds-slider_vertical');
  });

  it('should handle disabled state', () => {
    const fixture = createTestComponent(`<ngl-slider [value]="value" [disabled]="disabled"></ngl-slider>`);
    const { nativeElement, componentInstance } = fixture;

    const input = getSliderInput(nativeElement);
    expect(input.disabled).toEqual(false);

    componentInstance.disabled = true;
    fixture.detectChanges();
    expect(input.disabled).toEqual(true);
  });

  it('should support `ngModel`', () => {
    const fixture = createTestComponent(`<ngl-slider [ngModel]="value" (ngModelChange)="change($event)"></ngl-slider>`);
    const { nativeElement, componentInstance } = fixture;
    const input = getSliderInput(nativeElement);

    componentInstance.value = 50;
    fixture.detectChanges();
    fixture.whenStable().then(() => {
      fixture.detectChanges();
      expect(input.value).toEqual('50');
      expect(getSliderValue(nativeElement)).toEqual('50');

      input.value = '25';
      dispatchEvent(input, 'input');
      expect(componentInstance.change).toHaveBeenCalledWith(25);
    });
  });

  it('should handle `ngModel` disabled', () => {
    const fixture = createTestComponent(`<ngl-slider [ngModel]="value" disabled></ngl-slider>`);
    const input = getSliderInput(fixture.nativeElement);
    expect(input.disabled).toEqual(true);
  });

  it('should display error message', () => {
    const fixture = createTestComponent(`<ngl-slider error="I have error!"></ngl-slider>`);

    const slider = getSliderElement(fixture.nativeElement);
    expect(slider).toHaveCssClass('slds-has-error');

    const error = slider.querySelector('div.slds-form-element__help');
    expect(error.textContent).toEqual('I have error!');
    const input = getSliderInput(fixture.nativeElement);
    expect(input.getAttribute('aria-describedby')).toEqual(error.id);
  });
});

@Component({
  template: `<ngl-slider [value]="value" (valueChange)="change($event)"></ngl-slider>`,
})
export class TestComponent {
  value = null;
  size: string;
  label: string;
  disabled: boolean;

  change = jasmine.createSpy('sliderChange');
}