import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';

import { User } from '../../models/user';

import { firebaseConfig } from '../../app/app.firebase.config'
import { AuthService } from '../../services/auth.service';
import { AngularFireAuth } from 'angularfire2/auth';
/**
 * Generated class for the RegisterPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-register',
  templateUrl: 'register.html',
})
export class RegisterPage {
  user = {} as User;

  constructor(private fire: AngularFireAuth, private auth: AuthService, public navCtrl: NavController, public navParams: NavParams) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad LoginPage');
  }

  async register(user: User) {
    try {
      const result = await this.fire.auth.createUserWithEmailAndPassword(user.email, user.password)
        .then(data => {
          console.log("got data: ", data);
        }).catch(error => {
          console.error(error);
        });
      if (result) {
        this.navCtrl.setRoot('MenuPage');
      }
    }
    catch (e) {
      console.error(e);
    }

    console.log("Register user info: ", user);
    this.navCtrl.setRoot('MenuPage');
  }
}