import { AbstractControl, ValidationErrors, ValidatorFn } from '@angular/forms';

export function passwordValidator(control: AbstractControl): ValidationErrors | null {
    // one uppercase at least
    const passwordRegex = RegExp('(?=.*[A-Z])');
    const valid = passwordRegex.test(control.value);

    const errors = {
        password: {
            rules: 'doit contenit au moins une majuscule'
        }
    };

    return !valid ? errors : null;
}

export function forbidenExtensionValidator(forbidenExtension:string): ValidatorFn {
  // Check if control value ends with extension given in parameter
  return (control: AbstractControl): ValidationErrors | null => {
    // Now tested with extension given as parameter :)
    const isValid = (control.value as string).endsWith(forbidenExtension);

    if (!isValid) {
      // 'extension' is the error key
      return { 'extension': { value: control.value, expected: forbidenExtension} };
    } else {
      return null;
    }
  };
}
/*
export function checkEqualityValidator(controlName1:string, controlName2:string): ValidatorFn {
  return (control: AbstractControl): ValidationErrors | null => {
    // Get first control value
    if (controlName1.length > 0 ) {
      const value1 = control.get(controlName1).value;
    } else {
      const value1 = 'ko1';
    }
       // Get second control value
    const value2 = control.get(controlName2).value;

    if (!(value1 && value2 && value1 === value2)) {
      return { 'notEqual': { actual: value1, expected: value2 } };
    } else {
      return null;
    }
  };
}
*/