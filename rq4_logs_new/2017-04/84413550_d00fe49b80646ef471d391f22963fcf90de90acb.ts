import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import {ProductDescPage} from '../product-desc/product-desc';

/*
  Generated class for the Searchlist page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-searchlist',
  templateUrl: 'searchlist.html'
})
export class SearchlistPage {
	  product: any;
	  public subproducts: any;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
  	this.product= navParams.get('product');
  	this.getData();
  }

  
getData(){
 			this.subproducts = this.product.products;
            console.log(this.subproducts);
}

productDetailHandler(event, subproduct) {
  //alert(subproduct);
    this.navCtrl.push(ProductDescPage,
      {
        
        subproduct: subproduct.url
        
      });
  };


  ionViewDidLoad() {
    console.log('ionViewDidLoad SearchlistPage');
    
  }

}










