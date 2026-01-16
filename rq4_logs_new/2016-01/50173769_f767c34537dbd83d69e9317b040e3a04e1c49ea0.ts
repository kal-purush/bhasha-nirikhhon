/*
 * Angular
 */
/// <reference path="../../../typings/tsd.d.ts" />
/// <reference path="../../../typings/jwt-decode/jwt-decode.d.ts" />

import {Component, Injector, OnInit} from 'angular2/core';
import {Router, RouterLink, CanActivate, ComponentInstruction} from 'angular2/router';
import {Observable} from 'rxjs/Observable';
import {Http, Response, RequestOptions, Headers, HTTP_PROVIDERS} from 'angular2/http';
import {status, json} from '../utils/fetch';
import 'rxjs/Rx';
import {UserService} from'../services/UserService';
import {AuthService} from'../services/AuthService';
import {AuthHttp, JwtHelper} from 'angular2-jwt';
import {isLoggedIn} from '../services/is-logged-in';

let jwtDecode = require('jwt-decode');

export class User {
  constructor(
    public id: string,
    public username: string,
    public email: string,
    public password: string
    ){}
}

@Component({
  selector: 'login',
  viewProviders: [HTTP_PROVIDERS],
  template: `
  <div class="alert alert-danger" role="alert" *ngIf="message">
    {{ message }}
  </div>
  Logged in: {{ _authService.logged }}<br>
  <div *ngIf="!_authService.logged" class="signin-bar">
    <form class="form-inline" (submit)="login($event, username.value, password.value)">
      <div class="form-group">
        <label for="username">User:</label>
        <input value="{{user.email}}" class="form-control" name="username" #username>
      </div>
  
      <div class="form-group">
        <label for="password">Password:</label>
        <input value="{{user.password}}" class="form-control" type="password" name="password" #password>
      </div>
  
      <button class="btn btn-default" type="submit">
        Login
      </button>
      
    </form>
    Not registered yet? <a [routerLink]="['/Register']"> Signup</a>
  </div>  
<br>

<div *ngIf="_authService.logged" class="well">
  
  <span>Logged in as</span> 
 
  {{user.email}} | <b>{{user.username}}</b> 
  <button (click)="logout($event)">Log out</button>
  
  
</div>

  `,
  directives: [RouterLink]
})
//@CanActivate(() => tokenNotExpired())
export class LoginComponent implements OnInit{
  //static BASE_URL: string = 'http://localhost:3000/api';
  //logged: boolean;
  user: User;
  opts: RequestOptions;
  supersecret: string = 'luigi';
  jwtHelper: JwtHelper = new JwtHelper();
  
  constructor(
    public router: Router, 
    public http: Http, 
    private _userService: UserService,
    public _authService: AuthService) {
    this.user = new User('','','test@test.com', 'password');
  }

  
  login(event, email, password) {
    event.preventDefault();
     this._authService.login(email, password).subscribe(
			data => {
			//console.log(data);
			let r = json(data);
			console.log('yesss', json(data));
			// Set JWT to local storage
			//localStorage.setItem('id_token', r.token);	
			// Decode JWT to catch User Id
			//let decoded = this.jwtHelper.decodeToken(r.token).__data;
			//console.log(decoded);
			//let userId = decoded.userId;
			//this.getUser(userId);
			
      /*this._userService.getUser(userId)
        .subscribe((res: any) => {
          this.renderUser(res.json());
        
      });*/
      
      this.getUser();
        
      },
        err => console.log('error'),
        () => console.log('Request Complete')
        );
     
  }

  
  logout(event) {
    event.preventDefault();
    this._authService.logout().subscribe( () => {
      this.user = new User('','','test@test.com', 'password');
    } );
    
  }
  
  ngOnInit(): any {
    
    this._authService.verifyLogged();
    if(this._authService.logged === true) {
      this.getUser();
    } else {
      return;
    }

  }
  
  getUser() {
    
    if(this._authService.logged === true) {
      
      this._userService.getUser().subscribe(
            res => {console.log(json(res)); this.renderUser(res.json());},
            err => {
              console.log('error');
          },
            () => console.log('Request Complete')
            );
    } else {
      return;
    }      
  }

  
  renderUser(res: any): void {
    this.user = res;
    console.log(this.user);
  }

  signup(event) {
    event.preventDefault();
    this.router.navigate(['/Register']);
  }
  /*
  register(email, password) {
     this._authService.register(email, password);
  }
  */
  isLogged() {
    let bool;
    //bool = this._authService.checkToken();
    bool = this._authService.check();  
    console.log(bool);
    return bool;
  }
}