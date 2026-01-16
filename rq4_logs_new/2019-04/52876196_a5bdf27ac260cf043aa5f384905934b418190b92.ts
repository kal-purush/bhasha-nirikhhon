import { TestBed, ComponentFixture, async } from '@angular/core/testing';
import { Component } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { createGenericTestComponent, selectElements, dispatchEvent } from '../../../test/util';
import { NglColorpickerModule } from './module';
import { getRgbFromHex } from './util';
import { getPopoverElement } from '../popovers/popover.spec';

const createTestComponent = (html?: string, detectChanges?: boolean) =>
  createGenericTestComponent(TestComponent, html, detectChanges) as ComponentFixture<TestComponent>;

function getSummaryInput(element: HTMLElement): HTMLInputElement {
  return element.querySelector('.slds-color-picker__summary-input input');
}

function getSummaryButton(element: HTMLElement): HTMLButtonElement {
  return element.querySelector('.slds-color-picker__summary-button');
}

function getSummaryButtonColor(element: HTMLElement): string {
  return getSwatch(getSummaryButton(element)).style.background;
}

function getSwatch(element: HTMLElement): HTMLElement {
  return element.querySelector('.slds-swatch');
}

function getSwatches(element: HTMLElement): HTMLElement[] {
  return selectElements(element, '.slds-color-picker__swatch > a');
}

function getTabs(element: HTMLElement): HTMLElement[] {
  return selectElements(element, '.slds-tabs_default__link');
}

function getRGBStyle(hex) {
  const { red, green, blue } = getRgbFromHex(hex);
  return `rgb(${red}, ${green}, ${blue})`;
}

describe('`NglColorpicker`', () => {

  beforeEach(() => TestBed.configureTestingModule({ declarations: [TestComponent], imports: [NglColorpickerModule, FormsModule, ReactiveFormsModule] }));

  it('should render correctly', async(() => {
    const fixture = createTestComponent();
    const { nativeElement } = fixture;

    expect(nativeElement.firstElementChild).toHaveCssClass('slds-color-picker');

    fixture.whenStable().then(() => {
      expect(getSummaryInput(nativeElement).value).toBe('#ffddee');
      expect(getSummaryButtonColor(nativeElement)).toContain(getRGBStyle('#ffddee'));
    });
  }));

  it('should update based on model changes', async(() => {
    const fixture = createTestComponent();
    const { componentInstance, nativeElement } = fixture;

    fixture.whenStable().then(() => {
      componentInstance.color = '#111222';
      fixture.detectChanges();
      fixture.whenStable().then(() => {
        expect(getSummaryInput(nativeElement).value).toBe('#111222');
        expect(getSummaryButtonColor(nativeElement)).toContain(getRGBStyle('#111222'));
      });
    });
  }));

  it('should update appropriately if model is empty', async(() => {
    const fixture = createTestComponent();
    const { componentInstance, nativeElement } = fixture;

    fixture.whenStable().then(() => {
      componentInstance.color = null;
      fixture.detectChanges();
      fixture.whenStable().then(() => {
        expect(getSummaryInput(nativeElement).value).toBe('');
        expect(getSummaryButtonColor(nativeElement)).toEqual('');
      });
    });
  }));

  it('should send updates based on input box value', async(() => {
    const fixture = createTestComponent();
    const { componentInstance, nativeElement } = fixture;

    fixture.whenStable().then(() => {
      const inputEl = getSummaryInput(nativeElement);
      inputEl.value = '#111222';
      dispatchEvent(inputEl, 'input');

      fixture.detectChanges();
      fixture.whenStable().then(() => {
        expect(getSummaryButtonColor(nativeElement)).toContain(getRGBStyle('#111222'));
        expect(componentInstance.onChange).toHaveBeenCalledWith('#111222');
      });
    });
  }));

  it('should handle invalid updates of input box value', async(() => {
    const fixture = createTestComponent();
    const { componentInstance, nativeElement } = fixture;

    fixture.whenStable().then(() => {
      const inputEl = getSummaryInput(nativeElement);
      inputEl.value = 'invalid';
      dispatchEvent(inputEl, 'input');

      fixture.detectChanges();
      fixture.whenStable().then(() => {
        expect(getSummaryButtonColor(nativeElement)).toContain(getRGBStyle('#ffddee')); // keep previous state
        expect(componentInstance.onChange).toHaveBeenCalledWith(null);
        expect(nativeElement.querySelector('.slds-form-error')).toHaveText('Please ensure value is correct');
      });
    });
  }));

  it('should handle appropriately disable state', async(() => {
    const fixture = createTestComponent(`<ngl-colorpicker [ngModel]="color" disabled></ngl-colorpicker>`);
    const { nativeElement } = fixture;

    fixture.whenStable().then(() => {
      expect(getSummaryInput(nativeElement).disabled).toBe(true);
      expect(getSummaryButton(nativeElement).disabled).toBe(true);
    });
  }));

  it('should handle appropriately disable state', async(() => {
    const fixture = createTestComponent(`<ngl-colorpicker [ngModel]="color" readonlyInput="true"></ngl-colorpicker>`);
    const { nativeElement } = fixture;

    fixture.whenStable().then(() => {
      expect(getSummaryInput(nativeElement).readOnly).toBe(true);
      expect(getSummaryInput(nativeElement).disabled).toBe(false);
      expect(getSummaryButton(nativeElement).disabled).toBe(false);
    });
  }));

  describe('popover', () => {

    function openPopover(fixture) {
      return fixture.whenStable().then(() => {
        const button = getSummaryButton(fixture.nativeElement);
        button.click();
        fixture.detectChanges();
        return getPopoverElement();
      });
    }

    function pressDone(popover: HTMLElement) {
      const button = popover.querySelector('.slds-color-picker__selector-footer > button.slds-button_brand') as HTMLButtonElement;
      button.click();
    }

    function pressCancel(popover: HTMLElement) {
      const button = popover.querySelector('.slds-color-picker__selector-footer > button.slds-button_neutral') as HTMLButtonElement;
      button.click();
    }

    it('should render correctly', () => {
      const fixture = createTestComponent();
      openPopover(fixture).then((popover) => {
        expect(popover).toHaveCssClass('slds-popover');
        expect(popover).toHaveCssClass('slds-color-picker__selector');
      });
    });

    it('should send pressed color from swatches on done', async(() => {
      const fixture = createTestComponent();
      openPopover(fixture).then((popover) => {
        const swatches = getSwatches(popover);
        expect(swatches.length).toBe(28);

        swatches[0].click(); // #e3abec
        swatches[1].click(); // #c2dbf7
        pressDone(popover);
        expect(fixture.componentInstance.onChange).not.toHaveBeenCalledWith('#e3abec');
        expect(fixture.componentInstance.onChange).toHaveBeenCalledWith('#c2dbf7');
      });
    }));

    it('should not send pressed color from swatches on cancel', async(() => {
      const fixture = createTestComponent();
      openPopover(fixture).then((popover) => {
        const swatches = getSwatches(popover);
        swatches[0].click();
        swatches[1].click();
        pressCancel(popover);
        expect(fixture.componentInstance.onChange).not.toHaveBeenCalled();
      });
    }));

    it('should send custom color based on hex input', async(() => {
      const fixture = createTestComponent();
      openPopover(fixture).then((popover) => {
        const tabs = getTabs(popover);
        tabs[1].click();
        fixture.detectChanges();

        const hexInput = popover.querySelector('.slds-color-picker__input-custom-hex input');
        expect(hexInput.value).toEqual('#ffddee');

        hexInput.value = '#0000ff';
        dispatchEvent(hexInput, 'input');
        fixture.detectChanges();
        fixture.whenStable().then(() => {
          pressDone(popover);
          fixture.detectChanges();
          expect(getSummaryButtonColor(fixture.nativeElement)).toContain(getRGBStyle('#0000ff'));
          expect(fixture.componentInstance.onChange).toHaveBeenCalledWith('#0000ff');
        });
      });
    }));

    it('should send custom color based on RGB inputs', async(() => {
      const fixture = createTestComponent();
      openPopover(fixture).then((popover) => {
        const tabs = getTabs(popover);
        tabs[1].click();
        fixture.detectChanges();

        const rgbInputs = selectElements(popover, '.slds-input[maxlength="3"]') as HTMLInputElement[];
        expect(rgbInputs.length).toBe(3);
        expect(rgbInputs.map(e => e.value)).toEqual(['255', '221', '238']);

        rgbInputs.forEach((e, index) => {
          e.value = `${index}`;
          dispatchEvent(e, 'input');
        });
        fixture.detectChanges();
        fixture.whenStable().then(() => {
          pressDone(popover);
          fixture.detectChanges();
          expect(getSummaryButtonColor(fixture.nativeElement)).toContain(getRGBStyle('#000102'));
          expect(fixture.componentInstance.onChange).toHaveBeenCalledWith('#000102');
        });
      });
    }));

  });

});

@Component({
  template: `
    <ngl-colorpicker [ngModel]="color" (ngModelChange)="onChange($event)"></ngl-colorpicker>
  `,
})
export class TestComponent {
  color = '#ffddee';

  onChange: jasmine.Spy = jasmine.createSpy();
}