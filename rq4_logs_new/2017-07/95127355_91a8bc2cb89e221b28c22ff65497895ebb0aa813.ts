import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { AlertController } from 'ionic-angular';
import { MyAccountPage } from "../my-account/my-account";


/**
 * Generated class for the EditPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */
@IonicPage()
@Component({
  selector: 'page-edit',
  templateUrl: 'edit.html',
})
export class EditPage {

  constructor(public navCtrl: NavController, public navParams: NavParams, public alertCtrl: AlertController) {
  }
  ionViewDidLoad() {
    console.log('ionViewDidLoad EditPage');
  }
  showPrompt() {
    let prompt = this.alertCtrl.create({
      title: 'Change Password',
      inputs: [
        {
          name: 'old password',
          placeholder: 'Old Password',
        },
        {
          name: 'new password',
          placeholder: 'New Password',
        },
        {
          name: 'confirm password',
          placeholder: 'Confirm Password',
        }
      ],
      buttons: [
        {
          text: 'Cancel',
          handler: data => {
            console.log('Cancel clicked');
          }
        },
        {
          text: 'Save',
          handler: data => {
            console.log('Saved clicked');
          }
        }
      ]
    });
    prompt.present();
  }
  goToMyAccountPage() {
    this.navCtrl.push(MyAccountPage);
  }
}