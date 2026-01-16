import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

@Component({
  selector: 'page-list',
  templateUrl: 'preparo.html'
})
export class PreparoPage {
  selectedItem: any;
  items: Array<{title: string, note: string, icon: string}>;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
    // If we navigated to this page, we will have an item available as a nav param
    this.selectedItem = navParams.get('item');

   var prensaFrancesa = {
    title: 'Prensa Francesa' ,
    note: 'Descricao da Prensa Francesa' ,
    icon: 'ios-water-outline'
  }
  var coadoHarioV60 = {
    title: 'Hario V60' ,
    note: 'Descricao da Hario V60' ,
    icon: 'ios-water-outline'
  }
 
    this.items = [prensaFrancesa,coadoHarioV60];
  }

  itemTapped(event, item) {
    // That's right, we're pushing to ourselves!
    this.navCtrl.push(PreparoPage, {
      item: item
    });
  }
}