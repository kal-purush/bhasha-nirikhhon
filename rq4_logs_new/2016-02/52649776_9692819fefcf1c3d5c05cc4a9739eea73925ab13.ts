import { Component } from 'angular2/core';
import {FORM_DIRECTIVES, Validators, NgFormModel, ControlGroup, Control} from 'angular2/common';
import {Backand} from './backand/backand';

@Component({ 
  selector: 'app-container',
  templateUrl: 'login.html',
  directives: [FORM_DIRECTIVES],
  providers: [Backand]
})

export class AppComponent {
  loginForm: ControlGroup;
  
  constructor(public backand: Backand) {
	 this.loginForm = new ControlGroup({
      username: new Control('', Validators.required),
      password: new Control('', Validators.required)
    });
  }
  signIn(login){
    let auth = login.value;
    
    this.backand.signIn(auth.username, auth.password).subscribe(
      data => {
        this.backand.auth_status = 'OK';
        this.backand.is_auth_error = false;
        this.backand.setTokenHeader(data);
      },
      err => {
        var errorMessage = this.backand.extractErrorMessage(err);

        this.backand.auth_status = `Error: ${errorMessage}`;
        this.backand.is_auth_error = true;
        this.backand.logError(err);
      },
      () => console.log('Finish Auth'));
  }
}