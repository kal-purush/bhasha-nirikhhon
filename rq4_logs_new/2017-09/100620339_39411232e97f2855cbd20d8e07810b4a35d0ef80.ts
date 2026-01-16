import { User } from '../model/user';
import { Component } from '@angular/core';
import { OnInit} from '@angular/core';
import { LoginService } from '../service/login.service';
import { TokenService } from '../service/token.service';
import { Token } from '../model/token';
import { Router} from '@angular/router';
@Component({
    selector: 'app-login',
    templateUrl: 'login.component.html'
})

export class LoginComponent implements OnInit {
public user: User;

    constructor(private loginService: LoginService, private tokenService: TokenService, private router: Router) {}

 ngOnInit() {
     this.user = new User();
 }
    public login() {
        this.loginService.login(this.user).subscribe((token: Token) => {
            this.tokenService.token = token; // token stored
          //  this.router.navigate(['/overview']);
        });


    }


}