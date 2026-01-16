import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';

import { Venue } from '../../models/venue';

import {  WdiconfVenues } from '../../providers/wdiconf-venues';

/*
  Generated class for the Venues page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-venues',
  templateUrl: 'venues.html'
})
export class VenuesPage {

  venues: Venue[]

  constructor(public navCtrl: NavController, private wdiconfVenues: WdiconfVenues) {
    wdiconfVenues.load().subscribe(venues => {
      this.venues = venues;
})
  }

  ionViewDidLoad() {
    console.log('Hello VenuesPage Page');
  }

}