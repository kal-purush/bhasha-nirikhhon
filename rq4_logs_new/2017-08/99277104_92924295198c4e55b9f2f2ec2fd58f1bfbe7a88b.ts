import {AbstractControl, NG_VALIDATORS, Validator} from '@angular/forms';
import {Attribute, Directive, forwardRef} from '@angular/core';
import {UserServer} from '../user-server/user-server';

@Directive({
  selector: '[validateLogin][formControlName],[validateLogin] \n' +
  '    [formControl],[validateLogin][ngModel]',
  providers: [
    {provide: NG_VALIDATORS, useExisting: forwardRef(() => ValidateLoginDirective), multi: true}
  ]
})
export class ValidateLoginDirective implements Validator {
  private bool: any;
  constructor(@Attribute('validateLogin') public validateLogin: string,
              public userService: UserServer) {
  }

  validate(c: AbstractControl): { [p: string]: any } {
    const login = c;
    this.userService.getLogin(login.value).subscribe(data => {
      this.bool = data;
      if (this.bool === true) {
        login.setErrors({'validateLogin': data});
      } else {
        return null;
      }
  });
    return null;
}
}