import { Component } from '@angular/core';
import { AlertController, IonicPage, NavController, NavParams } from 'ionic-angular';

/**
 * Generated class for the AddPacientePage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-add-paciente',
  templateUrl: 'add-paciente.html',
})
export class AddPacientePage {

  constructor(private alertCtrl: AlertController, public navCtrl: NavController, public navParams: NavParams) {
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad AddPacientePage');
  }

  confirmarGuardar() {
    let alert = this.alertCtrl.create({    
      title: 'Guardar Paciente?',
      message: 'Está seguro que quiere guardar el paciente?', 
      buttons: [
        {
          text: 'Si', 
          handler: () => {
            console.log('Si')
          }
        }, 
        {
          text: 'No', 
          handler: () => {
            console.log('Si')
          }
        }
      ]
    });    

    alert.present();
  }
}