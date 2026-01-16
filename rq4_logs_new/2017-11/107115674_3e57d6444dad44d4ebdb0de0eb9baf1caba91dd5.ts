import { Component, OnInit } from '@angular/core';
import { ProductSerivces } from "./app.services";
import { DataService } from "./share-service";

@Component({
  selector: 'content-container',
  templateUrl: './content.component.html',
  styleUrls: ['./content.component.css']
})
export class ConentComponent implements OnInit {
  products:any = [];
  snacksProd:any = [];
  fruitsProd: any = [];
  kitchenProd: any = [];
  otherProd: any = [];
  tabCats:any = ['snacks', 'fruitsAndVeg', 'kitchen', 'others'];
  constructor(private productservice: ProductSerivces, private data: DataService) {  }
  ngOnInit() {
    this.getallproducts();
    this.getSnacksPrd(this.tabCats[0], 'specialOfferProducts');
    this.getFruitsPrd(this.tabCats[1], 'speacialOfferProducts');
    this.getKitchenPrd(this.tabCats[2], 'specialOfferProducts');
    this.getOtherPrd(this.tabCats[3], 'specialOfferProducts');
    this.getTodayOfferPrd('todayOfferProducts');
    
  }
  getallproducts() {
   this.productservice.getProducts().subscribe(
     products => {
       this.products = products
      },
     err => { if(err) console.log(err + " Something went wrong!!")},
     () =>{ }
   ) 
  }

  getSnacksPrd(cat, offer_type) {
    this.productservice.getOfferProd(cat, offer_type).subscribe(
      products => {
        this.snacksProd = products
       },
      err => { if(err) console.log(err + " Something went wrong!!")},
      () =>{ }
    )
  }

  getFruitsPrd(cat, offer_type) {
    this.productservice.getOfferProd(cat, offer_type).subscribe(
      products => {
        this.fruitsProd = products
       },
      err => { if(err) console.log(err + " Something went wrong!!")},
      () =>{ }
    )
  }

  getKitchenPrd(cat, offer_type) {
    this.productservice.getOfferProd(cat, offer_type).subscribe(
      products => {
        this.kitchenProd = products
       },
      err => { if(err) console.log(err + " Something went wrong!!")},
      () =>{ }
    )
  }

  getOtherPrd(cat, offer_type) {
    this.productservice.getOfferProd(cat, offer_type).subscribe(
      products => {
        this.otherProd = products
       },
      err => { if(err) console.log(err + " Something went wrong!!")},
      () =>{ }
    )
  }

  getTodayOfferPrd(offer_type) {
    this.productservice.getOfferProd('', offer_type).subscribe(
      products => {
        this.products = products
       },
      err => { if(err) console.log(err + " Something went wrong!!")},
      () =>{ }
    )
  }
  addtocart($event) {
    this.data.changeCart($event);
  }

}