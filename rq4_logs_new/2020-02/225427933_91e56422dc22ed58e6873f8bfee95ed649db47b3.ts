import { User } from './../../security/login/user.model';
import { LoginService } from './../../security/login/login.service';
import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'mt-user-detail',
  templateUrl: './user-detail.component.html',
  styleUrls: ['./user-detail.component.css']
})
export class UserDetailComponent implements OnInit {

  constructor(private loginService:LoginService) { }

  ngOnInit() {
  }

  user():User{
    return this.loginService.user
  }

  isLoggedIn():boolean{
    return this.loginService.isLoggedIn()
  }

  logout():void{
    this.loginService.logout()
  }

  login():void{
    this.loginService.handleLogin()
  }

}