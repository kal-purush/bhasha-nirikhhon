import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Produce } from '../produce-list/produce';

/**
 * Generated class for the OrderProducePage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@Component({
  selector: 'page-order-produce',
  templateUrl: 'order-produce.html',
})
export class OrderProducePage {
  item = {} as Produce;
  //private pallets : number = 0;
  Tab : string = "Details";

  constructor(public navCtrl: NavController, public navParams: NavParams) {
    this.item = navParams.data;
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad OrderProducePage');
  }

  getItems(type : any)
  {
    return [];
  }

}