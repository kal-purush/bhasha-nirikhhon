import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams, Events } from 'ionic-angular';
import { AuthProvider } from '../../providers/auth/auth';


@IonicPage()
@Component({
  selector: 'page-login',
  templateUrl: 'login.html',
})
export class LoginPage {

  constructor(
    public navCtrl: NavController, 
    public navParams: NavParams, 
    public events: Events, 
    private authProvider: AuthProvider
  ) {
  }

  loginWithAuth0() {
    this.authProvider.login().then((res) => {
      this.events.publish('logged-in', {authResult: res});
      this.navCtrl.setRoot('HomePage');
    }, (error) => {
      console.log(error);
    });
  }

}