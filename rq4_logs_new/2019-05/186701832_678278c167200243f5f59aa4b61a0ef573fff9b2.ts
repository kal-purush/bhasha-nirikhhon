import { Directive, Input } from '@angular/core';
import { Validator, AbstractControl, ValidationErrors, NG_VALIDATORS } from '@angular/forms';

@Directive({
  selector: '[appDateValidator]',
  providers: [{ provide: NG_VALIDATORS, useExisting: DateValidatorDirective, multi: true }]
})
export class DateValidatorDirective implements Validator {


  constructor() { }

  validate(control: AbstractControl): ValidationErrors | null {
    const strDateDepart: string = control.value;
    const dateDepart = new Date(strDateDepart);
    if (dateDepart > new Date()) {
      return null;
    }
    return { dateInvalide: true };
    /*
    if (photoUrl !== null && photoUrl.startsWith('http')) {
      return null;
    } else {
      return { urlInvalide: true };
    }
    */
   return null;
  }


}