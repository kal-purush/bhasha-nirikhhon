import {Component} from '@angular/core';
import {NavController, NavParams} from 'ionic-angular';
import { InAppBrowser } from '@ionic-native/in-app-browser';

@Component({
  selector: 'accomodation',
  templateUrl: 'accomodation.html'
})
export class AccomodationPage {

  constructor(public nav: NavController, navParams: NavParams, private iab: InAppBrowser) {

  }

  itemTapped(url: string) {
    const browser = this.iab.create(url, '_system', '');
    browser.show();
  }
}