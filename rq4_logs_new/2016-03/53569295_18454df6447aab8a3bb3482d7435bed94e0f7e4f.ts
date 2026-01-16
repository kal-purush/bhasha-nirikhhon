import {Component} from 'angular2/core';
import {FORM_BINDINGS, FORM_DIRECTIVES} from 'angular2/common';
import {ControlGroup, FormBuilder, Validators} from 'angular2/common';

@Component({
  selector: 'my-app',
  viewBindings: [FORM_BINDINGS],
  template: require('./login.html'),
  directives: [FORM_DIRECTIVES]
})
export class LoginComponent {
  ngOnInit() {
    console.log('ngOnInit(): LoginComponent');
  }
  loginForm: ControlGroup;
  constructor(builder: FormBuilder) {
    this.loginForm = builder.group({
      login: ["", Validators.required],
      passwordRetry: builder.group({
        password: ["", Validators.required],
        // passwordConfirmation: ["", Validators.required, asyncValidator]
        passwordConfirmation: ["", Validators.required]
      })
    });
  }
  get value(): string {
    return JSON.stringify(this.loginForm.value, null, 2);
  }
}