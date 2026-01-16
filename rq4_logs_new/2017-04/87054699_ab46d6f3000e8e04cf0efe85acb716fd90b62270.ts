import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { Cart } from '../../providers/checkin-cart';

/*
  Generated class for the CheckinCart page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-checkin-cart',
  templateUrl: 'checkin-cart.html'
})
export class CheckinCartPage {

  items: any;
  final: Number;

  constructor(public navCtrl: NavController, public navParams: NavParams, public cart: Cart) {
    this.final = 0;
    console.log(this.cart.items)
    this.items = this.cart.items;
    this.initFinal();
  }
  quantIncr(id){
    for(let i=0; this.items.length-1 >= i; i++){
      if(this.items[i].id == id){
        this.items[i].quant = this.items[i].quant+1;
        this.items[i].amountss = this.items[i].quant*this.items[i].price + this.items[i].additPrice
      }
    }
    this.initFinal();
   }
  quantDicr(id){
    for(let i=0; this.items.length-1 >= i; i++){
      if(this.items[i].quant > 1){
        if(this.items[i].id == id){
          this.items[i].quant = this.items[i].quant-1;
          this.items[i].amountss = this.items[i].quant*this.items[i].price + this.items[i].additPrice
        }
      }
    }
    this.initFinal();
  }
  remove(id){
    if(this.items.length == 0){
      for(let i=0; this.items.length-1 >= i; i++){
        if(this.items[i].id == id){
          this.items.splice(i, 1);
        }
      }
      console.log(this.items.length);
      this.initFinal();
      this.cart.count = this.cart.count - 1;
    }else{
      console.log('asdasd');
      this.cart.status = false;
      this.navCtrl.pop(); 
    }
    
  }
  initFinal(){
    this.final = 0;
    for(let i=0; this.items.length-1 >= i; i++){
      this.final += this.items[i].amountss;
    }
    console.log(this.final);
  }
  ionViewDidLoad() {
    console.log('ionViewDidLoad CheckinCartPage');
  }

}