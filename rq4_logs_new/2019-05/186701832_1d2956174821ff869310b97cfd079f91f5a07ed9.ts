import { Directive } from '@angular/core';
import { NG_VALIDATORS, AbstractControl, ValidationErrors, Validator } from '@angular/forms';

@Directive({
  selector: '[appImmatriculationValidator]',
  providers: [{ provide: NG_VALIDATORS, useExisting: ImmatriculationValidatorDirective, multi: true }]
})
export class ImmatriculationValidatorDirective implements Validator {

  constructor() { }

  validate(control: AbstractControl): ValidationErrors | null {
    let immatriculation = control.value;


    return null;
  }

}