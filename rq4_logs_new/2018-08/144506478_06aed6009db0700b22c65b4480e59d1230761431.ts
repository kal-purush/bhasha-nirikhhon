import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

@Component({
  selector: 'grao-list',
  templateUrl: 'grao.html'
})
export class GraoListPage {
  selectedItem: any;
  items: Array<{title: string, note: string, icon: string}>;

  constructor(public navCtrl: NavController, public navParams: NavParams) {
    // If we navigated to this page, we will have an item available as a nav param
    this.selectedItem = navParams.get('item');

   var arabica = {
    title: 'Grão Arábica' ,
    note: 'Descricao do Grão Arábica' ,
    icon: 'ios-leaf-outline'
  }
  var robusta = {
    title: 'Grão Robusta' ,
    note: 'Descricao do Grão Robusta' ,
    icon: 'ios-leaf-outline'
  }
 
    this.items = [arabica,robusta];
  }

  itemTapped(event, item) {
    // That's right, we're pushing to ourselves!
    this.navCtrl.push(GraoListPage, {
      item: item
    });
  }
}