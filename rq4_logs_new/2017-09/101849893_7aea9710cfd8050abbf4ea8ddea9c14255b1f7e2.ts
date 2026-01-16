import { Component } from '@angular/core';
import { IonicPage, NavController, NavParams } from 'ionic-angular';
import { RevmaxProvider as Revmax } from '../../providers/revmax';

/**
 * Generated class for the ProductDetailPage page.
 *
 * See http://ionicframework.com/docs/components/#navigation for more info
 * on Ionic pages and navigation.
 */

@IonicPage(
  {
    name: 'product-detail',
    segment: 'product-detail/:id'
  }
)
@Component({
  selector: 'page-product-detail',
  templateUrl: 'product-detail.html',
})
export class ProductDetailPage {
  public productId;
  public product;
  constructor(public navCtrl: NavController, public navParams: NavParams, public Revmax: Revmax) {
    this.productId = this.navParams.get("id");
    console.log(this.productId);
  }

  showProductDetails(){
    this.Revmax.fetchProductInfo(this.productId);
    this.product = this.Revmax.productInfo;
    console.log('here are the products');
    console.log(this.product);

    if(this.product){
      this.product = this.Revmax.productInfo;
    }
    else{
      console.log('in else');
      this.Revmax.categoryProductSubject.subscribe((val)=>{
        console.log('in subs');      
        console.log(val);
        this.products = val;      
            });
      }
    }
  }

}