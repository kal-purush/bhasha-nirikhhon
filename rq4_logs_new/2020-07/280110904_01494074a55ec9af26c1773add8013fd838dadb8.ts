import { Directive, Input } from '@angular/core';
import { NG_VALIDATORS, Validator, AbstractControl, ValidationErrors, ValidatorFn, FormGroup } from '@angular/forms';

@Directive({
  selector: '[appConfirmInput]',
  providers:[{provide:NG_VALIDATORS,useExisting:ConfirmValiditorDirective,multi:true}]
})
export class ConfirmValiditorDirective implements Validator{

  validate(control: AbstractControl): ValidationErrors | null {
    console.log(identityRevealedValidator(control))
    return identityRevealedValidator(control);
  }
  registerOnValidatorChange?(fn: () => void): void {
    throw new Error("Method not implemented.");
  }

  
}

export const identityRevealedValidator: ValidatorFn = (control:FormGroup):ValidationErrors|null=>{ 
  const name = control.get('name');
  const confirmName = control.get('confirmName');

  return name && confirmName && name.value === confirmName.value ? { 'identityRevealed': true } : null;
}