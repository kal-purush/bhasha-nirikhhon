import { Component } from '@angular/core';
import { NavController, ViewController } from 'ionic-angular';

/*
  Generated class for the SellerAwardModalPage page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  templateUrl: 'build/pages/seller-award-modal/seller-award-modal.html',
})
export class SellerAwardModalPage {
    award: Object = {};
    
    constructor(
        private nav: NavController,
        private view: ViewController
    ) {}

    /**
     * Closes the modal
     */
    dismiss() {
        this.view.dismiss();
    }
}