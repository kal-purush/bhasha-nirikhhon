
import { Component } from '@angular/core';
import { NavController, NavParams, MenuController } from 'ionic-angular';
import { AuthService } from '../../services/authentication-service';

/*
  Generated class for the Login page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
    selector: 'page-user-register',
    templateUrl: 'user-register.html'
})
export class UserRegisterPage {

    public errors: any = [];
    private userEmail: string;
    private userPassword: string;
    private userConfirmPassword: string;

    constructor(public authService: AuthService, public navCtrl: NavController,
        public navParams: NavParams, public menuCtrl: MenuController) {
        this.menuCtrl.enable(false);
    }

    ionViewDidLoad() {
        console.log('ionViewDidLoad LoginPage');
    }

    registerUser() {
        this.authService.registerUser(this.userEmail, this.userPassword, this.userConfirmPassword).subscribe(
            data => {
                this.navCtrl.popToRoot()
            },
            error => {
                console.log(error);
                this.errors.push(error);
            });

    }

}