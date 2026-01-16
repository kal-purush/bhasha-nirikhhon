import { Injectable } from '@angular/core';
import {AuthService} from "./auth.service";

@Injectable({
  providedIn: 'root'
})
export class LoginService {

  constructor(
      private authService: AuthService,
  ) { }

    logedIn: boolean = false;


    async logIn(): Promise<boolean> {
        if (localStorage.getItem("loginToken") != null) {
            const res: any = await this.authService.validateToken().toPromise();
            this.logedIn=true
            return res.valid;
        } else {
            return false;
        }
    }

  }


