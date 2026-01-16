import { Component } from '@angular/core';
import { NavController, AlertController } from 'ionic-angular';

import { HomePage } from '../home/home';

@Component({
  selector: 'page-login',
  templateUrl: 'login.html'
})
export class LoginPage {

  invalid:boolean = false;
  bamaId:string = '';
  password:string = '';

  constructor(public navCtrl: NavController,  public alertCtrl: AlertController) {

  }

  login(){
    console.log("Bama ID: "+ this.bamaId)
    console.log("Password: "+ this.password)

    if(this.bamaId === '' || this.password === ''){
        let alert = this.alertCtrl.create({
          title:'Login Error',
          subTitle:'All fields are rquired',
          buttons:['OK']
        });
        alert.present();
        return;
      }

    else if(this.bamaId === 'nlsaban' && this.password === 'rolltide'){
      this.navCtrl.push(HomePage);
    }
  }
}