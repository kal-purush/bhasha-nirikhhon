import { Component } from '@angular/core';
import { NavController, NavParams } from 'ionic-angular';
import { WdiconfEvents } from '../../providers/wdiconf-events';
import { Event } from '../../models/event';

/*
  Generated class for the EventDetails page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  selector: 'page-event-details',
  templateUrl: 'event-details.html'
})
export class EventDetailsPage {
  event: Event;
  event_id: number;

  constructor(public navCtrl: NavController, private navParams: NavParams, private wdiconfEvents: WdiconfEvents) {
    this.event_id = navParams.get('id');
    wdiconfEvents.loadDetails(this.event_id).subscribe(event => {
      this.event = event;
    })
  }

  ionViewDidLoad() {
    console.log('Hello EventDetailsPage Page');
  }

}