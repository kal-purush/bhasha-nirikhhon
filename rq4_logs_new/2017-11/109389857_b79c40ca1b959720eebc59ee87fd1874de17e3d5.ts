import { Component } from '@angular/core';
import { LoadingController, IonicPage, NavController, NavParams } from 'ionic-angular';
import { TurnosApi } from '../../shared/turnos-api'

/**
 * Generated class for the TurnosPage page.
 *
 * See https://ionicframework.com/docs/components/#navigation for more info on
 * Ionic pages and navigation.
 */

@IonicPage()
@Component({
  selector: 'page-turnos',
  templateUrl: 'turnos.html',
})
export class TurnosPage {
  private turnos: any;
  constructor(private loadingController: LoadingController,public navCtrl: NavController, public navParams: NavParams, private turnosApi: TurnosApi) {
  }

  ionViewDidLoad() {
    let loader = this.loadingController.create({
      content: 'Getting data...'
    });

    loader.present().then(() => {
      this.turnosApi.getTurnos().subscribe(data => {        
        this.turnos = data.turnoes;                
        loader.dismiss();
      });
    });
  }
  
  itemTapped(event, item) {
    console.log(item);
    
  }
}