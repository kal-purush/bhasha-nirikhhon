/// <reference path="../../typings/tsd.d.ts" />

import {
  Component,
  EventEmitter,
  Inject,
  OnInit,
  FormBuilder,
  Validators,
  ControlGroup,
  FORM_DIRECTIVES
} from 'angular2/angular2';

import {User} from 'client/user/user.js';

@Component({
  selector: 'login-cmp',
  providers: [User],
  templateUrl: 'client/login/login.html',
  styleUrls: ['client/login/login.css'],
  outputs: ['loginOk'],
  directives: [FORM_DIRECTIVES]
})
export class LoginCmp implements OnInit {
    loginOk: EventEmitter = new EventEmitter();
    loginForm: ControlGroup;

    constructor(@Inject(FormBuilder) public fb: FormBuilder,
                @Inject(User) public user: User) {

      this.loginForm = fb.group({
        "user": [this.user.name, Validators.required]
      });
    }

    onInit() {
      console.log('login-cmp ok');
    }

    login(user: string) {
      console.log(user);
    }

    escHandler() {
      this.loginForm.controls.user.updateValue("");
    }
}