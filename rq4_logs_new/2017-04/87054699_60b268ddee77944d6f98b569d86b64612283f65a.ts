import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';


import { Storage } from '@ionic/storage';


/*
  Generated class for the CheckinDish page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-checkin-dish',
  templateUrl: 'checkin-dish.html'
})
export class CheckinDishPage {

  dish: any;
	dishCurr: any;
	dishType: any;
  cart: any;
  constructor(public navCtrl: NavController, public navParams: NavParams, public storage: Storage) {
  	this.initializeDish();
  	
  	this.dishCurr = {
      label:'ssss', 
      id: 1, 
      src: 'http://onklik.kz/upload/Bookingmenu/full/Bookingmenu-image-temp43026-0.jpg', 
      price: 2700, 
      count: false, 
      excerpt: 'Салат со свеклой, руколой и козьим сыром',
      prefs: [
        {id:1, title: 'Острый'},
        {id:2, title: 'Неострый'},
      ],
      addits: [
        {id:1, title:'С сыром', price: 100},
        {id:2, title:'С сыром', price: 500},
        {id:3, title:'С сыром', price: 200}
      ]
    };
    this.dishType = {
      pref: '',
      addit: '',
      quant: 1,
      additPrice: 0,
      amountss: this.dishCurr.price
    };

    storage.ready().then(() => {

      storage.get('checkinCart').then((val) => {
        this.cart = JSON.parse(val);
      });
     });
    
  }
  quantIncr(){
    this.dishType.quant = this.dishType.quant+1;
    this.dishType.amountss = this.dishCurr.price*this.dishType.quant+ this.dishType.additPrice;
  }
  quantDicr(){
    if(this.dishType.quant > 1){
      this.dishType.quant = this.dishType.quant-1;
      this.dishType.amountss = this.dishCurr.price*this.dishType.quant+ this.dishType.additPrice;
    }
  }
  refresh(){
    this.dishType.additPrice = 0;
    for(let i = 0; i <= this.dishCurr.addits.length-1; i++){
      for(let x = 0; x<= this.dishType.addit.length; x++){
        if( this.dishCurr.addits[i].id == this.dishType.addit[x] ){
          this.dishType.additPrice += this.dishCurr.addits[i].price;
        }
      }
    }
    console.log(this.dishType.additPrice);
    this.dishType.amountss = this.dishCurr.price*this.dishType.quant+ this.dishType.additPrice;
  }
  addToCart(){
    this.cart.status = true;
    this.storage.set('checkinCart', JSON.stringify(this.cart));
    this.storage.get('checkinCart').then((val) => {
      console.log(JSON.parse(val));
    });
  }
  getDishInfo(id, array){
  	for (var i = array.length - 1; i >= 0; i--) {
  		if(array[i].id == id){
  			this.dishCurr = array[i];
  		}
  	}
  }
  initializeDish(){
  	this.dish = [
	  {label:'ssss', id: 1, src: 'http://onklik.kz/upload/Bookingmenu/full/Bookingmenu-image-temp43026-0.jpg', price: 2700, count: false, excerpt: 'Салат со свеклой, руколой и козьим сыром'},
	  {label:'dddd', id: 2, src: 'http://onklik.kz/upload/Bookingmenu/full/Bookingmenu-image-temp43026-0.jpg', price: 2740, count: false, excerpt: 'Салат со свеклой, руколой и козьим сыром'},
	  {label:'cccc', id: 3, src: 'http://onklik.kz/upload/Bookingmenu/full/Bookingmenu-image-temp43026-0.jpg', price: 2740, count: false, excerpt: 'Салат со свеклой, руколой и козьим сыром'},
	  {label:'zzzz', id: 4, src: 'http://onklik.kz/upload/Bookingmenu/full/Bookingmenu-image-temp43026-0.jpg', price: 2720, count: false, excerpt: 'Салат со свеклой, руколой и козьим сыром'}
    ];
  }
  ionViewDidLoad() {
  }

}