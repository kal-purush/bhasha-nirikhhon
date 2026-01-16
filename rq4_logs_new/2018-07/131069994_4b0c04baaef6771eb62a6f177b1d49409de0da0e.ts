import { Component, Input } from '@angular/core';
import { NavController, ActionSheetController } from 'ionic-angular';

import { Event } from '../../../../_models/event';

@Component({
  selector: 'bar-events-card',
  templateUrl: 'bar-events-card.html'
})

export class BarEventsCardComponent {

  @Input() event: Event;
  @Input() confirmEventCb: Function;
  @Input() removeEventCb: Function;

  constructor(
    public navCtrl:NavController,
    public actionSheetCtrl:ActionSheetController
  ) {}

}