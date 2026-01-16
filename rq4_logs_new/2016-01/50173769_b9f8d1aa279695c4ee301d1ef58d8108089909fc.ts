/*
 * Angular
 */
/// <reference path="../../../typings/tsd.d.ts" />
/// <reference path="../../../typings/jwt-decode/jwt-decode.d.ts" />

import {Component, Injector, OnInit} from 'angular2/core';
import {ROUTER_DIRECTIVES, Router, RouterLink, CanActivate} from 'angular2/router';
import {Observable} from 'rxjs/Observable';
import {Http, Response, RequestOptions, Headers, HTTP_PROVIDERS} from 'angular2/http';
import {status, json} from '../utils/fetch';
import 'rxjs/Rx';
import {UserService} from'../services/UserService';
import {AuthService} from'../services/AuthService';
import {AuthHttp, AuthConfig, JwtHelper} from 'angular2-jwt';
import {LoginComponent} from'./LoginComponent';

export class User {
  constructor(
    public id: string,
    public username: string,
    public email: string,
    public password: string
    ){}
}

@Component({
  selector: 'register',
  template: `

<form class="form-inline" (submit)="register(newUsername.value, newPassword.value)">
    <div class="form-group">
      <label for="username">User:</label>
      <input class="form-control" name="username" #newUsername>
    </div>

    <div class="form-group">
      <label for="password">Password:</label>
      <input class="form-control" type="password" name="password" #newPassword>
    </div>

    <button class="btn btn-default" type="submit">
       Register
    </button>
    
  </form>
<br>
  `,
  directives: [ROUTER_DIRECTIVES]
})
export class RegisterComponent {
  user: User;
  constructor(
    public router: Router, 
    public http: Http, 
    private _userService: UserService,
    private _authService: AuthService
    
    ) {
      this.user = new User('','','test@test.com', 'password');
  }
  
  register(email, password) {
     this._authService.register(email, password).subscribe( (res) => {
       
       let r = json(res);
       console.log('Registration successful! ',r);
       // Login After Registration
       this._authService.login(email, password).subscribe( (res) => {
         let r = json(res);
         console.log('Login after Registration successed', r);
         // Get User
         
         this._userService.getUser().subscribe(
            res => {console.log(json(res)); this.renderUser(res.json());},
              err => {
                console.log('error');
            },
              () => console.log('Request Complete')
            );
          
       });
       
     });
     
     //this.loginAfterRegister(email, password);
     
  }
  
  renderUser(res: any): void {
    this.user = res;
    console.log(this.user);
  }  
  
  loginAfterRegister(email, password) {
    this._authService.login(email, password).subscribe( (res) => {
      console.log('Login after Registration successed');
    });
  }
  
}