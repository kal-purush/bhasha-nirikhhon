import { FormControl } from '@angular/forms';

export class PasswordValidator {

  static isValid(control: FormControl): any {

    console.log(control);
    function validPass(password) {
      let re = /^\w+$/;
      if(password) {
        if (password.length < 6) {
          return false;
        }
      }
      else {
        return false;
      }
      re = /[0-9]/;
      if (!re.test(password)) {
        return false;
      }
      re = /[a-zA-Z]/;
      if (!re.test(password)) {
        return false;
      }
      return true;
    }
    if (!validPass(control.value)) {
      return {
        "Passwords must contain at least six characters, including letters and numbers.": true
      };
    }
    return null;
  }

  static confirmPass(control: FormControl): any {
    if (control.value == control.root.value['fcpass']) {
      return null;
    }
    else {
        return { isValid: true };
    }
  }
}