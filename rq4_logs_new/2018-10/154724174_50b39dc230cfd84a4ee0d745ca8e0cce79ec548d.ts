import { Injectable } from '@angular/core';
import { AuthService, SocialUser } from 'angularx-social-login';

@Injectable({
  providedIn: 'root'
})
export class UserStatusService {
    private user: SocialUser;
    private isLoggedIn:boolean = false;

  constructor(private authService:AuthService) {

    this.authService.authState.subscribe((user) => {
        this.user = user;
        this.isLoggedIn = (user != null) ? true : false;
        console.log(this.user, this.isLoggedIn);
      });  
   }

   getUser():SocialUser{
       return this.user;
   }

   isUserLoggedIn():boolean{
        return this.isLoggedIn;
   }

}