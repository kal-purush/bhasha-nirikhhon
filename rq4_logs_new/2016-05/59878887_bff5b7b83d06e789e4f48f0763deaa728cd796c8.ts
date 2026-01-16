import {Component} from 'angular2/core';
import {ControlGroup, Control, Validators, FormBuilder} from 'angular2/common';
import {UsernameValidators} from './usernameValidators';

@Component({
  selector: 'signup-form',
  templateUrl: 'app/signup-form.component.html'
})

export class SignUpFormComponent {
  form: ControlGroup;

  // import from commons
  constructor(fb: FormBuilder) {
    // store it in a form
    this.form = fb.group({
      // pass in array default value, validator, both optional
      // username: ['', Validators.required],
      username: ['', Validators.compose([
          Validators.required,
          UsernameValidators.cannotContainSpace
        ]), UsernameValidators.shouldBeUnique],
      password: ['', Validators.required]
    })
  }
  // form = new ControlGroup({
  //   // value?,
  //   username: new Control('', Validators.required),
  //   password: new Control('', Validators.required)
  // });

  signup() {
    // var result = authService.login(this.form.value);
    this.form.find('username').setErrors({
      invalidLogin: true
    });

    console.log(this.form.value);
  }
}