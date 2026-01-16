import { Component } from '@angular/core';
import { IonicPage, NavController } from 'ionic-angular';
import { User } from '../../interfaces';
import { AuthProvider } from '../../providers'

@IonicPage({
  name: 'LoginPage'
})
@Component({
  selector: 'page-login',
  templateUrl: 'login.html',
})
export class LoginPage {
  private _user: User = {
    email: null,
    password: null
  };

  constructor(
    private _auth: AuthProvider,
    public navCtrl: NavController
  ) { }

  private onLoginClick(user: User) {
    this._auth.loginWithEmail(user).subscribe(data => {
      this.navCtrl.push('HomePage');
    }, error => {
      alert('Login failed');
    });
  }

  private onSignUpClick(): void {
    this.navCtrl.push('SignUpPage');
  }

  private onResetPasswordClick(): void {
    this.navCtrl.push('ResetPasswordPage');
  }
}