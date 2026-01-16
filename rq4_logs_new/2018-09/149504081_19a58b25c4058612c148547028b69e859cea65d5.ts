import { ValidatorFn, ValidationErrors, AbstractControl } from '@angular/forms';

export class TriangleValidators {

  static positive(control: AbstractControl): ValidationErrors | null {
    return control.value > 0 ? null : { positive : 'nope' };
  }

  static triangle(control: AbstractControl): ValidationErrors | null {
    if (!(control.value.a * control.value.b * control.value.c)) {
      return null;
    }
    if (+control.value.a >= (+control.value.b + +control.value.c) ||
      +control.value.b >= (+control.value.a + +control.value.c) ||
      +control.value.c >= (+control.value.a + +control.value.b)) {
        return {triangle: 'nope'};
    }
    return null;
  }
}