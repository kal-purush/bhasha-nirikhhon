import { Component } from '@angular/core';
import { NavController, NavParams, Loading, LoadingController, AlertController } from 'ionic-angular';

import { AuthServiceProvider } from '../../providers/auth-service/auth-service';
import { ListPage } from '../list/list';

/**
 * Generated class for the LoginPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@Component({
  selector: 'page-login',
  templateUrl: 'login.html',
})

/**
 * Class Managing the Login Page of the application.
 */
export class LoginPage {
  // Loading screen for the login page.
  loading: Loading;
  // Credentials of the user.
  credentials = { email: '', password: ''};
  /**
   * @constructor
   * @param {NavController} navCtrl
   * @param {NavParams} navParams
   * @param {AuthServiceProvider} auth
   * @param {AlertController} alertCtrl
   * @param {LoadingController} loadingCtrl
   */
  constructor(public navCtrl: NavController,
    public navParams: NavParams,
    public auth: AuthServiceProvider,
    public alertCtrl: AlertController,
    public loadingCtrl: LoadingController) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad LoginPage');
  }
  /**
   * Shows the loading screen.
   */
  showLoading() {
    this.loading = this.loadingCtrl.create({
      content: 'Loading...',
      dismissOnPageChange: true
    });
    this.loading.present();
  }
  /**
   * Shows the alert window to the user.
   * @param {string} title - Title of the alert. Default is Alert.
   * @param {string} subTitle - Body of the alert.
   */
  showAlert(title:string='Alert', subTitle:string) {
    this.loading.dismiss();

    let alert = this.alertCtrl.create({
      title,
      subTitle,
      buttons: ['OK']
    });

    alert.present(prompt);
  }
  /**
   * Invoked when clicking the SignUp button.
   * Shows an alert for now.
   * Implementation pending.
   */
  public signUp() {
    this.showLoading();
    this.showAlert('Signup', 'Clicked SignUp');
  }
  /**
   * Invoked when clicking the login buttin.
   * Shows a loading screen and invokes the Auth Service.
   * If succesful from Auth Service, move to List View page.
   * If not succesful from Auth Service, shows an alert.
   */
  public login() {
    this.showLoading();
    this.auth.login(this.credentials).then((res) => {
      if(res) {
        this.navCtrl.setRoot(ListPage);
      } else {
        this.showAlert('Login Failed', 'Email or Password is wrong');
      }
    }).catch((err) => {
      this.showAlert('Error', err);
    });
  }

}