import {Attribute, Directive, forwardRef, Inject, Input} from '@angular/core';
import {AbstractControl, NG_VALIDATORS, Validator} from "@angular/forms";

@Directive({
  selector: '[validateEqual]',
  providers: [
    { provide: NG_VALIDATORS, useExisting: EqualValidatorDirective, multi: true }
  ]
})
export class EqualValidatorDirective implements Validator {
  constructor( @Attribute('validateEqual') public validateEqual: string) {}

  validate(c: AbstractControl): { [key: string]: any } {
    // self value (e.g. retype password)
    let v = c.value;

    // control value (e.g. password)
    let e = c.root.get(this.validateEqual);

    // value not equal
    if (e && v !== e.value) return {
      validateEqual: false
    };
    return null;
  }
}