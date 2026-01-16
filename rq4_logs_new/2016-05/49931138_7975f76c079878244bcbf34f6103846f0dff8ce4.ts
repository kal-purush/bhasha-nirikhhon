import {Component} from '@angular/core';
import { AppComponent } from './app.component';
import {ROUTER_DIRECTIVES, Routes, Router} from "@angular/router";
import {HomeComponent} from "./home.component";


declare const FB:any;

@Component({
    selector: 'facebook-login',
    templateUrl: 'app/facebooklogin.html',
    directives: [ROUTER_DIRECTIVES, AppComponent]
})

@Routes([
    {path: '/home',  component: HomeComponent}
])

export class FacebookLoginComponent {

    

    constructor(private router: Router) {
        FB.init({
            appId      : '1752803258266614',
            status     : true,
            cookie     : true,
            xfbml      : true,
            version    : 'v2.4'
        });
    }

    onFacebookLoginClick() {
        FB.login();
        FB.getLoginStatus(response => {
            this.statusChangeCallback(response);
        });
    }

    statusChangeCallback(resp) {
        if (resp.status === 'connected') {
            localStorage.setItem('userId', resp.authResponse.userID);
            localStorage.setItem('accessToken', resp.authResponse.accessToken);
            this.router.navigate(['/home']);
        }else if (resp.status === 'not_authorized') {
        }else {
        }
    };
    ngOnInit() {
        FB.getLoginStatus(response => {
            this.statusChangeCallback(response);
        });
    }
}