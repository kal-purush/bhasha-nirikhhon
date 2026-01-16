import {AbstractControl, NG_VALIDATORS, Validator} from '@angular/forms';
import {Attribute, Directive, forwardRef} from '@angular/core';
import {UserServer} from '../user-server/user-server';

@Directive({
  selector: '[validateEmail][formControlName],[validateEmail] \n' +
  '    [formControl],[validateEmail][ngModel]',
  providers: [
    {provide: NG_VALIDATORS, useExisting: forwardRef(() => ValidateEmailDirective), multi: true}
  ]
})
export class ValidateEmailDirective implements Validator {
  private bool: any;

  constructor(@Attribute('validateEmail') public validateEmail: string,
              private userService: UserServer) {
  }

  validate(c: AbstractControl): { [p: string]: any } {

  let value = c.value;
  var email = c;
    const regex = new RegExp(`^[a-z0-9]+(\.[_a-z0-9]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,15})$`);
    if (regex.test(value)) {

      this.userService.getEmail(value).subscribe(data => {
        this.bool = data;
        if (this.bool === true) {
         email.setErrors({'validateEmail': data});
        } else {
          return null;
        }
        }
      );

    } else {
      return null;
    }


  }

}