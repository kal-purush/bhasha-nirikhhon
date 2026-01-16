import { Component } from '@angular/core';

/**
 * Generated class for the BroadcastMessageListComponent component.
 *
 * See https://angular.io/docs/ts/latest/api/core/index/ComponentMetadata-class.html
 * for more info on Angular Components.
 */
@Component({
  selector: 'broadcast-message-list',
  templateUrl: 'broadcast-message-list.html'
})
export class BroadcastMessageListComponent {

  text: string;

  constructor() {
    console.log('Hello BroadcastMessageListComponent Component');
    this.text = 'Hello World';
  }

}