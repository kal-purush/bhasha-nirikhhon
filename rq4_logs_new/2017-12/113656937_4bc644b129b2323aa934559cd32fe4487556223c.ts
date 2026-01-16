import {Component, OnInit} from "@angular/core";
import {Router} from "@angular/router";
import {AuthData} from "../../entity/authdata";
import {RegisterData} from "../../entity/register-data";
import {UserService} from "../../service/user.service";
import {User} from "../../entity/user";

@Component({
  selector: 'app-auth',
  templateUrl: './auth.component.html',
  styleUrls: ['./auth.component.css']
})
export class AuthComponent implements OnInit {
  registerData: RegisterData = new RegisterData();
  authData: AuthData = new AuthData();

  user: User;

  constructor(private userService: UserService, private router: Router) {
  }

  ngOnInit() {
    this.userService.subscribe(user=>{
      this.getUserInfo();
    });

    this.getUserInfo();
  }

  isLogged() {
    return this.userService.isLogged();
  }

  signOut(): void{
    this.userService.signOut().subscribe(message => {
        console.log('Signed out');
      },
      error => {
        console.error(error);
      });
  }

  signUp(): void {
    this.userService.signUp(this.registerData).subscribe(user => {
        console.log('Signed up');
      },
      error => {
        console.error(error);
      })
  }

  getUserInfo(){
    if(this.isLogged()){
      this.userService.getMyUserInfo().subscribe(user => {
        console.log('Got user info');
        this.user = user;
      });
    }
  }

  signIn(): void {
    this.userService.signIn(this.authData).subscribe(token => {
        console.log('Signed in');
      },
      error => {
        console.error(error);
      })
  }
}