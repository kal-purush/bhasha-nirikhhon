import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

@Component({
  selector: 'page-stop-detail',
  templateUrl: 'stop-detail.html'
})
export class StopDetailPage {
  selectedItem: any;
  items: Array<{ destination: string}>;

  // TODO: 
  // 1. get current location data
  // 2. get data (all destination of the bus) returned from Translink API

  constructor(public nav: NavController, public navParams: NavParams) {
    this.selectedItem = navParams.get('title');

    // TODO: get destination by Translink API
    
    this.items = [{ destination: 'Water front'}, { destination: 'New Westminster Station'}];
  }
}