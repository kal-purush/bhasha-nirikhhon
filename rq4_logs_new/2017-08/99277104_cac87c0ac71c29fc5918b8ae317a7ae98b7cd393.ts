import {AbstractControl, NG_VALIDATORS, Validator} from '@angular/forms';
import {Attribute, Directive, forwardRef} from '@angular/core';

@Directive({
  selector: '[validateEqual][formControlName],[validateEqual] \n' +
  '    [formControl],[validateEqual][ngModel]',
  providers: [
    {provide: NG_VALIDATORS, useExisting: forwardRef(() => EqualsValidatorDirective), multi: true}
  ]
})
// Кастомныйы Валидатор
export class EqualsValidatorDirective implements Validator {
  // Беру атребут с таким именем
  constructor(@Attribute('validateEqual') public validateEqual: string,
              @Attribute('reverse') public reverse: string) {
  }

  private get isReverse() {
    if (!this.reverse) return false;
    return this.reverse === 'true';
  }

  // Беру поле в котором этот атрибут
  validate(pas: AbstractControl): { [p: string]: any } {
    debugger
    const confirmPassword = pas.value;
    // Плднимаюсь в корень и беру поле c которым мы сравниваем совпадение
    const password = pas.root.get(this.validateEqual);
    // Если не совпали то ошибка
    if (confirmPassword !== password.value && !this.isReverse) {
      return {
        validateEqual: false
      };
    }

    // value equal and reverse
    if (confirmPassword === password.value && this.isReverse) {
      delete password.errors['validateEqual'];
      if (!Object.keys(password.errors).length) {
        password.setErrors(null);
      }
    }

    // value not equal and reverse
    if (confirmPassword !== password.value && this.isReverse) {
      password.setErrors({validateEqual: false});
    }

    return null;
  }


}