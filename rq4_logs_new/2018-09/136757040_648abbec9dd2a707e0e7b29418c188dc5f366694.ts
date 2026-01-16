import { Component, forwardRef } from '@angular/core';
import { NG_VALUE_ACCESSOR, ControlValueAccessor } from '@angular/forms';

@Component({
  selector: 'app-custom-date',
  template: `
    <input type="text" (change)="writeValue($event.target.value)" (focus)="onFocus()"/>
  `,
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DateControlComponent),
      multi: true
    }
  ]
})
export class DateControlComponent implements ControlValueAccessor {
  value = '';

  onChange: (value: string) => {};
  onTouched = () => {};

  writeValue(newValue: any): void {
    if (typeof newValue !== 'string') {
      return;
    }

    this.value = newValue;
    this.onChange(this.value);
  }

  registerOnChange(fn: any): void {
    this.onChange = fn;
  }

  registerOnTouched(fn: any): void {
    this.onTouched = fn;
  }

  onFocus() {
    this.onTouched();
  }
}