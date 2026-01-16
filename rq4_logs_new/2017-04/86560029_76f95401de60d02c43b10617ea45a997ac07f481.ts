import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { PoetDetail } from '../../components/poet-detail'
import { IPoet } from "../models/IPoet";
/*
  Generated class for the PoetDetail page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-poet-detail',
  templateUrl: 'poet-detail.html'
})
export class PoetDetailPage {
  public poet: IPoet;
  constructor(public navCtrl: NavController, public navParams: NavParams) {
      this.poet=navParams.get("poet");

  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad PoetDetailPage');
  }

}