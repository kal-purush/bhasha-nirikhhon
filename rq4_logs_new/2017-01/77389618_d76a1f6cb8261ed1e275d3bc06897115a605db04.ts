import { Component, ViewChild } 	  from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

import { AppConfig } 		       	    from '../../../app/app.config';

import { QueryInput }               from '../../query-input.model';
import { Product }                  from '../product.model';
import { PRODUCT, ProductService }  from '../product.service';

import { ProductDetailsPage }       from '../product-details/product-details';


@Component({
  selector    : 'page-product-search',
  templateUrl : 'product-search.html',
  providers   : [PRODUCT]
})
export class ProductSearchPage {
  @ViewChild('searchbar') myInput;

  public  products         : Array<Product>;
  public  originalProducts : Array<Product>;
  public  queryInput       : QueryInput     = {
    page: 1
  };
  public picture     = null;
  public showSpinner = true;

  constructor(
  	public navCtrl   : NavController,
  	private $product : ProductService) {
  }


  //FUNCTIONS --------------------------------------
  public query(): void{
    this.queryInput.page = 1;
    this.products        = null;

  }
  
  public search(searchEvent): void {
    this.showSpinner    = true;
    let term            = searchEvent.target.value;
    let params : Object = {
      include    : 'images',
      data       : term
    }

    if (term.trim() === '' || term.trim().length < 3) {
      this.products = this.originalProducts;
    } else {
      this.$product.search(params).then(
      data => {
        this.products    = <Array<Product>> data;
        this.showSpinner = false;
      });
    }
  }







  //COMPONENTS ----------------------------------------
  private ionViewDidEnter(): void{
     setTimeout(() => {
      this.myInput.setFocus();
    }, 150);     
  }

  public doInfinite(infiniteScroll): void {
  	this.queryInput.page = this.queryInput.page + 1;

  	this.$product.query(this.queryInput).then(
  		data => {
  			this.products = this.products.concat(<Array<Product>> data);
  			infiniteScroll.complete();
  		}
    );  	
  }






  //NAV ----------------------------------------------------
  public goProductDetails(productId: number): void{
    this.navCtrl.push(ProductDetailsPage, {id: productId});
  }

}