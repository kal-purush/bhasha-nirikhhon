import { Component, OnInit } from '@angular/core';
import {NavController, LoadingController, NavParams} from 'ionic-angular';


@Component({
  selector: 'list-events-tab',
  templateUrl: 'list-events-tab.html'
})
export class ListEventsTab implements OnInit{

  events: any;
  confirmEventCb: Function;
  removeEventCb: Function;
  emptyCollectionMessage: String;

  constructor(public navCtrl: NavController, navParams: NavParams) {
    console.log(navParams.data);
    this.events =  navParams.data.events;
    this.confirmEventCb = navParams.data.confirmEventCb;
    this.removeEventCb = navParams.data.removeEventCb;
    this.emptyCollectionMessage = navParams.data.emptyCollectionMessage;
  }

  async ngOnInit() {
  }


}