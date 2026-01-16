import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

/*
  Generated class for the Circuitos page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-circuitos',
  templateUrl: 'circuitos.html'
})
export class CircuitosPage {

	titulo: string;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  	this.titulo = 'Circuito '+this.navParams.get('titulo');
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad CircuitosPage');
  }

}