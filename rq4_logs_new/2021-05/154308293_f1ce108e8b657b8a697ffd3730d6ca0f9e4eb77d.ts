import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  forwardRef,
} from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';

import { Field } from '@firestitch/field-editor';


@Component({
  selector: 'app-terms-field-config',
  templateUrl: './terms-field-config.component.html',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => TermsFieldConfigComponent),
      multi: true,
    },
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TermsFieldConfigComponent implements ControlValueAccessor {

  private _onChange: (value: unknown) => void;
  private _onTouch: (value: unknown) => void;

  private _disabled = false;
  private _field: Field = null;

  constructor(
    private _cdRef: ChangeDetectorRef,
  ) {
  }

  public get disabled(): boolean {
    return this._disabled;
  }

  public get field(): Field {
    return this._field;
  }

  public writeValue(obj: Field | undefined): void {
    this._field = obj;
    this._cdRef.markForCheck();
  }

  public setDisabledState(isDisabled: boolean): void {
    this._disabled = isDisabled;
  }

  public fieldChange(field: Field): void {
    this._field = field;
    this._onChange(field);
  }

  public registerOnChange(fn: any): void {
    this._onChange = fn;
  }

  public registerOnTouched(fn: any): void {
    this._onTouch = fn;
  }

}