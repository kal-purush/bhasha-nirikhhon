import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { Storage } from '@ionic/storage';

/*
  Generated class for the Cart page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-cart',
  templateUrl: 'cart.html'
})
export class CartPage {

  constructor(public navCtrl: NavController, public navParams: NavParams, private storage: Storage) {}

  ionViewDidLoad() {
    //var test = JSON.parse(storage.get('CartItem'));
    let obtainedItems_array = JSON.parse(this.storage.get('CartItem'));  
    console.log(JSON.parse(obtainedItems_array));  
    this.storage.get('CartItem').then((value)=>{
     console.log('CartItem');
  })
}
}
