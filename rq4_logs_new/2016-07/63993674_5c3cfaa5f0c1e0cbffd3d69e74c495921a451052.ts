import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

@Component({
  templateUrl: 'build/pages/product-info/product-info.html',
})
export class ProductInfoPage {
  productName: string;
  productSku: string;
  image: Array<{ name: string }>;
  items = [];
  loadFinish: boolean = false;

  constructor(private nav: NavController, private navParams: NavParams) {
    this.productName = this.navParams.get('productName');
    this.productSku = this.navParams.get('productSku');
    this.image = [
      { name: 'D01.jpg' },
      { name: 'D02.jpg' },
      { name: 'D03.jpg' },
      { name: 'D05.jpg' },
      { name: 'D06.jpg' },
      { name: 'D07.jpg' },
      { name: 'D08.jpg' },
      { name: 'D09.jpg' },
      { name: 'D10.jpg' },
      { name: 'D15.jpg' }
    ];
  }

  slideOptions = {
    initialSlide: 0,
    loop: true,
    autoplay: 2000
  };

  doInfinite(infiniteScroll) {
    setTimeout(() => {
      this.items.push(this.image[this.items.length]);
      this.loadFinish = this.items.length == this.image.length;
      infiniteScroll.complete();
    }, 500);
  }

}