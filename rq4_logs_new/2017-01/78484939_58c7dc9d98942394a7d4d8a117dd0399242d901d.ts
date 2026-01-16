import { Component } from '@angular/core';
import { Geolocation } from 'ionic-native';
import { NavController, NavParams, Loading } from 'ionic-angular';

import { Translink } from '../../../providers/translink';

import { StopDestinationPage } from '../stop-destination/stop-destination'

@Component({
  selector: 'page-nearby-list',
  templateUrl: 'nearby-list.html'
})
export class NearbyListPage {
  loading: Loading;
  selectedItem: any;
  stops: any = {};
  items: Array<{title: string, destination: string, distance: string}>;

  constructor(public nav: NavController, public navParams: NavParams, public translink: Translink) {
    //this.loading = this._createLoading();

    // Get current geolocation
    Geolocation.getCurrentPosition({
      timeout:5000, enableHighAccuracy:true
    }).then((resp) => {
      this.getItems(resp.coords.latitude, resp.coords.longitude);  // Using current geolocation info to get all items.
    }).catch((err) => {
      console.log('Fail getting location', err);
    });
    this.selectedItem = navParams.get('item');
  }

  /**
   * Call Translink API to get all nearby stops in 500 meters.
   * @param 
   *        lat: latitude of current location
   *        lng: longitude of current location
   */
  getItems(lat, lng) {

    this.translink.nearbyStopsByLocation(lat.toFixed(6), lng.toFixed(6)).then(data => {
      this.stops = data;
    })
    // TODO:  assign the return data to items.
    // for(let i = 1; i < this.stops.length()+1; i++) {
    //     this.items.push({
    //         title: this.stops.number,
    //         destination: this.stops.destination,
    //         distance: this.stops.distance,
    //     });
    // }
    this.items = [{ title: '123', destination: 'Waterfront', distance: '50'}, { title: '106', destination: 'Nanamo', distance: '100' }];
  } 

  // TODO: show list on google map
  showOnMap() {
    // this.nav.push(ShowMapPage, { data: this.items });
  }

  itemTapped(event, item) {
    this.nav.push(StopDestinationPage, {
      item: item
    })
  }
}