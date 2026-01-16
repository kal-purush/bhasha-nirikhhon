import { AbstractControl, NG_VALIDATORS, Validator } from '@angular/forms';
import { Attribute, Directive, forwardRef } from '@angular/core';

@Directive({
  // selector: '[appEqualValidator]',
  selector: '[validateEqual][formControlName],[validateEqual][formControl],[validateEqual][ngModel]',
  providers: [
    { provide: NG_VALIDATORS, useExisting: forwardRef(() => EqualValidatorDirective), multi: true }
]
})
export class EqualValidatorDirective implements Validator {
  constructor( @Attribute('validateEqual') public validateEqual: string,
  @Attribute('reverse') public reverse: string) {

}

  private get isReverse() {
    if (!this.reverse) {
      return false;
    }
    return this.reverse === 'true' ? true : false;
  }

  validate(password: AbstractControl): { [key: string]: any } {
    const passValue = password.value;


    const confirmPassValue = password.root.get(this.validateEqual);

    if (confirmPassValue && passValue !== confirmPassValue.value && !this.isReverse) {
      return {
        validateEqual: false
      };
    }

    if (confirmPassValue && passValue === confirmPassValue.value && this.isReverse) {
        delete confirmPassValue.errors['validateEqual'];
        if (!Object.keys(confirmPassValue.errors).length) {
          confirmPassValue.setErrors(null);
        }
    }

    if (confirmPassValue && passValue !== confirmPassValue.value && this.isReverse) {
      confirmPassValue.setErrors({
            validateEqual: false
        });
    }
    return null;
  }
}