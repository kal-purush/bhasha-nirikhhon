import { Component, forwardRef, input } from '@angular/core';
import {
  ControlValueAccessor,
  FormsModule,
  NG_VALUE_ACCESSOR,
  ReactiveFormsModule,
} from '@angular/forms';
import { CommonModule } from '@angular/common';
import { IonInput } from '@ionic/angular/standalone';

@Component({
  selector: 'app-custom-input',
  standalone: true,
  imports: [CommonModule, FormsModule, ReactiveFormsModule, IonInput],
  template: `
    <ion-input type="text" [ngModel]="value" (input)="updateValue($event)">
    </ion-input>
  `,
  styles: ``,
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => CustomInputComponent),
      multi: true,
    },
  ],
})
export class CustomInputComponent implements ControlValueAccessor {
  regexp = input.required<string>();
  value: string = '';
  onChange: any = () => {};
  onTouch: any = () => {};

  writeValue(value: any): void {
    this.value = value;
  }
  registerOnChange(onChange: any): void {
    this.onChange = onChange;
  }
  registerOnTouched(onTouched: any): void {
    this.onTouch = onTouched;
  }

  updateValue(event: Event): void {
    this.onTouch();
    const newValue = (event.target as HTMLInputElement).value;
    const check = new RegExp(this.regexp());
    if (check.test(newValue)) {
      this.value = newValue;
      this.onChange(newValue);
    }
  }
}