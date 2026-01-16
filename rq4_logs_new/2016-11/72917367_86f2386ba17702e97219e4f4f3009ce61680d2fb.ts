import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Http } from '@angular/http';

import 'rxjs/add/operator/map';

/*
  Generated class for the TulisKomentar page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-tulis-komentar',
  templateUrl: 'tulis-komentar.html'
})
export class TulisKomentarPage {
  public id;

  constructor(public navCtrl: NavController, public navParams: NavParams, public http: Http) {
  	this.id = navParams;
  }

  ionViewDidLoad() {
    console.log('Hello TulisKomentarPage Page');
  }



}