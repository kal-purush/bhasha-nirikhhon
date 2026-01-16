import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';

// import { StopDetailPage } from '../stop-detail/stop-detail';

@Component({
  selector: 'page-nearby-list',
  templateUrl: 'nearby-list.html'
})
export class NearbyListPage {
  selectedItem: any;
  items: Array<{title: string, distance: string}>;

  // TODO: 
  // 1. get current location data
  // 2. get data (nearby stops) returned from Translink API

  constructor(public nav: NavController, public navParams: NavParams) {
    this.selectedItem = navParams.get('item');

    // this.items = [];
    // for(let i = 1; i < buses.length(); i++) {
    //     this.items.push({
    //         title: buses.number,
    //         distance: buses.distance,
    //         icon: 'bus'
    //     });
    // }
    this.items = [{ title: '123', distance: '50'}, { title: '106', distance: '100' }];
  }

  showOnMap(event) {
    // this.nav.push(StopDetailPage, { data: this.items });
  }
}