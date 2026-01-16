import {FormControl} from '@angular/forms';

export function myRequired(control: FormControl) {
  if (control.value) {
    return null;
  } else {
    return {
      'required': {
        valid: false,
        errorMessage: 'Обязательное поле!'
      }
    };
  }
}

