import { Directive } from '@angular/core';
import { NG_VALIDATORS, Validator, AbstractControl, ValidatorFn } from '@angular/forms';

@Directive({
  selector: '[validEmail]',
  providers: [{provide: NG_VALIDATORS, useExisting: EmailvalidatorDirective, multi: true}]
})
export class EmailvalidatorDirective  implements Validator {

  validate(control: AbstractControl): {[key: string]: any} | null {
    const emailValue = control.value;
    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    const emailValid = emailRegex.test(emailValue);

    return emailValid ? null : { 'invalidEmail': true };
  }
}